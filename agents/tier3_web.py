"""
Tier 3 — fetch allowlisted URLs and detect experimentation / competitor keywords in page text.

Config: config/tier3_sources.yaml. Enable with TIER3_WEB_ENABLED=1.
"""

from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
import yaml
from bs4 import BeautifulSoup

from core.schema import AccountRecord

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "tier3_sources.yaml"


def _tier3_enabled() -> bool:
    v = (os.getenv("TIER3_WEB_ENABLED") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _load_config() -> Dict[str, Any]:
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    title_el = soup.title
    title = title_el.get_text(strip=True) if title_el else ""
    body = soup.get_text(separator=" ", strip=True)
    return f"{title}\n{body}" if title else body


def _page_title_only(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    t = soup.title
    return t.get_text(strip=True) if t and t.string else ""


def _snippet(text: str, idx: int, radius: int = 140) -> str:
    lo = max(0, idx - radius)
    hi = min(len(text), idx + radius)
    chunk = text[lo:hi].replace("\n", " ")
    chunk = re.sub(r"\s+", " ", chunk).strip()
    return chunk


def _looks_like_html(content_type: str, url: str) -> bool:
    ct = (content_type or "").strip().lower()
    if not ct:
        return True
    if "text/html" in ct or "application/xhtml" in ct:
        return True
    path = urlparse(url).path.lower()
    return path.endswith(".html") or path.endswith(".htm") or path.endswith("/")


def _matches_in_text(
    needles: List[str], haystack_lower: str
) -> Tuple[Set[str], Optional[int]]:
    """Return (matched original-case labels, index of earliest hit for snippet)."""
    matched_set: Set[str] = set()
    first_idx: Optional[int] = None
    for needle in needles:
        if not needle or not needle.strip():
            continue
        low = needle.lower()
        pos = haystack_lower.find(low)
        if pos >= 0:
            matched_set.add(needle)
            if first_idx is None or pos < first_idx:
                first_idx = pos
    return matched_set, first_idx


def _robots_allows(url: str, user_agent: str) -> bool:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return False
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        return True
    try:
        return rp.can_fetch(user_agent or "*", url)
    except Exception:
        return True


def _resolve_account_name(
    entry: Dict[str, Any], final_url: str, html: str
) -> str:
    explicit = (entry.get("company_name") or "").strip()
    if explicit:
        return explicit
    title = _page_title_only(html)
    if title:
        return title[:200]
    host = urlparse(final_url).netloc or "unknown"
    return host.replace("www.", "") or f"Unknown ({final_url})"


def _pick_urgency(has_competitor: bool, has_keyword: bool) -> str:
    if has_competitor and has_keyword:
        return "active"
    if has_competitor:
        return "active"
    if has_keyword:
        return "watch"
    return "watch"


async def collect() -> List[AccountRecord]:
    if not _tier3_enabled():
        print("[Tier3] Skipping — TIER3_WEB_ENABLED not set")
        return []

    if not _CONFIG_PATH.is_file():
        print(f"[Tier3] Skipping — missing {_CONFIG_PATH}")
        return []

    cfg = _load_config()
    fetch_cfg = cfg.get("fetch") or {}
    timeout = float(fetch_cfg.get("timeout_seconds", 30))
    max_bytes = int(fetch_cfg.get("max_response_bytes", 2_097_152))
    user_agent = str(
        fetch_cfg.get("user_agent") or "FigmentE100-Tier3/1.0"
    )
    delay = float(fetch_cfg.get("delay_between_requests_seconds", 0))
    respect_robots = bool(fetch_cfg.get("respect_robots_txt", False))

    keywords = [str(x).strip() for x in (cfg.get("keywords") or []) if str(x).strip()]
    competitors = [
        str(x).strip() for x in (cfg.get("competitors") or []) if str(x).strip()
    ]
    sources = cfg.get("sources") or []
    if not isinstance(sources, list):
        sources = []

    if not sources:
        print("[Tier3] No sources in tier3_sources.yaml — nothing to fetch")
        return []

    out: List[AccountRecord] = []

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers={"User-Agent": user_agent},
        follow_redirects=True,
    ) as client:
        for entry in sources:
            if not isinstance(entry, dict):
                continue
            url = (entry.get("url") or "").strip()
            if not url:
                continue

            if respect_robots:
                allowed = await asyncio.to_thread(_robots_allows, url, user_agent)
                if not allowed:
                    print(f"[Tier3] robots.txt disallows {url} — skip")
                    if delay > 0:
                        await asyncio.sleep(delay)
                    continue

            try:
                resp = await client.get(url)
            except httpx.HTTPError as e:
                print(f"[Tier3] Fetch error {url}: {e}")
                if delay > 0:
                    await asyncio.sleep(delay)
                continue

            status = resp.status_code
            if status != 200:
                print(f"[Tier3] HTTP {status} for {url}")
                if delay > 0:
                    await asyncio.sleep(delay)
                continue

            raw = resp.content
            if len(raw) > max_bytes:
                print(f"[Tier3] Response too large ({len(raw)} B) — skip {url}")
                if delay > 0:
                    await asyncio.sleep(delay)
                continue

            ctype = resp.headers.get("content-type") or ""
            if not _looks_like_html(ctype, str(resp.url)):
                print(f"[Tier3] Non-HTML response for {url} — skip")
                if delay > 0:
                    await asyncio.sleep(delay)
                continue

            try:
                html = raw.decode(resp.encoding or "utf-8", errors="replace")
            except Exception:
                html = raw.decode("utf-8", errors="replace")

            text = _html_to_text(html)
            lower = text.lower()

            kw_hit, kw_first = _matches_in_text(keywords, lower)
            comp_hit, comp_first = _matches_in_text(competitors, lower)

            if not kw_hit and not comp_hit:
                if delay > 0:
                    await asyncio.sleep(delay)
                continue

            first_idx = kw_first
            if first_idx is None:
                first_idx = comp_first
            snippet = _snippet(text, first_idx or 0) if first_idx is not None else ""

            account_name = _resolve_account_name(entry, str(resp.url), html)
            source_label = (entry.get("source_label") or "tier3_web").strip()

            competitor_field: Optional[str] = None
            if comp_hit:
                competitor_field = sorted(comp_hit, key=len, reverse=True)[0]

            deal_parts = [
                f"url={resp.url}",
                f"keywords={', '.join(sorted(kw_hit))}" if kw_hit else "",
                f"competitors={', '.join(sorted(comp_hit))}" if comp_hit else "",
            ]
            if snippet:
                deal_parts.append(f"snippet={snippet}")
            deal_context = "\n".join(p for p in deal_parts if p)

            t3_extras = {
                "source_url": str(resp.url),
                "matched_keywords": ", ".join(sorted(kw_hit)),
                "matched_competitors": ", ".join(sorted(comp_hit)),
                "http_status": str(status),
            }

            rec = AccountRecord(
                account_name=account_name,
                tier=3,
                source=source_label,
                competitor=competitor_field,
                urgency=_pick_urgency(bool(comp_hit), bool(kw_hit)),
                deal_context=deal_context,
                notes=f"Tier 3 web signal ({source_label})",
                tier3_extras=t3_extras,
            )
            out.append(rec)
            print(f"[Tier3] Hit on {resp.url} → account={account_name!r}")

            if delay > 0:
                await asyncio.sleep(delay)

    print(f"[Tier3] {len(out)} account(s) from web signals")
    return out
