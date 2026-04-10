"""
Load and resolve ``config/e100_output_columns.yaml`` for Sheets / CSV export.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from core.schema import AccountRecord

_MANIFEST_PATH = Path(__file__).resolve().parent.parent / "config" / "e100_output_columns.yaml"


def _is_blank_for_default(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, str) and not val.strip():
        return True
    return False


def load_e100_output_manifest(path: Optional[Path] = None) -> List[Dict[str, Any]]:
    p = path or _MANIFEST_PATH
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    cols = data.get("columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError(f"Invalid or empty columns in {p}")
    return cols


def manifest_headers(columns: List[Dict[str, Any]]) -> List[str]:
    return [str(c["header"]) for c in columns]


def resolve_manifest_cell(account: AccountRecord, spec: Dict[str, Any]) -> Any:
    if "field" in spec:
        val = getattr(account, str(spec["field"]), None)
        fmt = spec.get("format")
        if fmt == "percent_0_1":
            if val is None:
                return ""
            try:
                return f"{float(val):.1%}"
            except (TypeError, ValueError):
                return str(val)
        if spec.get("default") is not None and _is_blank_for_default(val):
            val = spec["default"]
        return val
    if "looker_extra" in spec:
        return account.looker_extras.get(str(spec["looker_extra"]), "")
    if "wisdom_extra" in spec:
        return account.wisdom_extras.get(str(spec["wisdom_extra"]), "")
    if "tier3_extra" in spec:
        return account.tier3_extras.get(str(spec["tier3_extra"]), "")
    raise ValueError(
        f"Column spec must include field, looker_extra, wisdom_extra, or tier3_extra: {spec}"
    )


def account_to_manifest_row(
    account: AccountRecord, columns: List[Dict[str, Any]]
) -> List[Any]:
    return [resolve_manifest_cell(account, col) for col in columns]
