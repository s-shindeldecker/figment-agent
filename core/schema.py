from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime


@dataclass
class AccountRecord:
    # Identity
    account_name: str
    sfdc_account_id: Optional[str] = None
    ld_account_id: Optional[str] = None

    # Commercial context
    arr: Optional[float] = None
    plan: Optional[str] = None
    rating: Optional[str] = None
    geo: Optional[str] = None
    industry: Optional[str] = None
    renewal_date: Optional[str] = None

    # Ownership
    ae: Optional[str] = None
    csm: Optional[str] = None

    # Tier assignment
    tier: Optional[int] = None          # 1, 2, or 3
    source: Optional[str] = None        # "looker", "enterpret", "external"

    # Tier 1 signals
    exp_events_mtd: Optional[float] = None
    exp_events_entitled: Optional[float] = None
    exp_utilisation_rate: Optional[float] = None
    is_using_exp_90d: Optional[bool] = None
    days_since_last_iteration: Optional[float] = None
    active_experiments: Optional[int] = None

    # Tier 2 signals
    competitor: Optional[str] = None
    competitor_spend: Optional[float] = None
    renewal_window_months: Optional[int] = None
    urgency: Optional[str] = None       # "immediate", "active", "watch"
    deal_context: Optional[str] = None  # summary from Enterpret

    # Scoring output
    expansion_score: Optional[float] = None
    priority_rank: Optional[int] = None

    # Override layer
    override_action: Optional[str] = None   # "include", "exclude", "deprioritize"
    override_reason: Optional[str] = None
    override_by: Optional[str] = None

    # Metadata
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    notes: Optional[str] = None

    # Unmapped / pass-through for wide export (Sheets manifest)
    looker_extras: Dict[str, str] = field(default_factory=dict)
    wisdom_extras: Dict[str, str] = field(default_factory=dict)
    tier3_extras: Dict[str, str] = field(default_factory=dict)
