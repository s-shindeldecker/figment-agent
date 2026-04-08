import json

from agents.prioritizer import (
    PRIORITIZER_USER_INTRO,
    _USER_PAYLOAD_SUFFIX,
    build_prioritizer_user_message,
)
from core.schema import AccountRecord


def test_build_prioritizer_user_message_includes_intro_accounts_and_suffix():
    accounts = [
        AccountRecord(account_name="Acme", tier=1, source="looker", arr=100000),
        AccountRecord(account_name="Beta", tier=2, source="enterpret", urgency="watch"),
    ]
    msg = build_prioritizer_user_message(accounts)
    assert msg.startswith(PRIORITIZER_USER_INTRO)
    assert msg.endswith(_USER_PAYLOAD_SUFFIX.strip())
    assert msg.rstrip().endswith(_USER_PAYLOAD_SUFFIX.strip())
    # JSON blob between intro and suffix
    inner = msg[len(PRIORITIZER_USER_INTRO) : -len(_USER_PAYLOAD_SUFFIX)]
    data = json.loads(inner.strip())
    assert len(data) == 2
    assert data[0]["account_name"] == "Acme"
    assert data[0]["tier"] == 1
    assert "urgency" not in data[0]
    assert data[1]["urgency"] == "watch"
