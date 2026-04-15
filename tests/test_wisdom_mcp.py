from agents.wisdom_mcp import (
    extract_json_array_from_text,
    records_from_wisdom_tool_result,
)


def test_extract_json_array_from_fenced_block():
    text = """Here you go:
```json
[{"account_name": "Acme", "urgency": "watch"}]
```
"""
    rows = extract_json_array_from_text(text)
    assert rows == [{"account_name": "Acme", "urgency": "watch"}]


def test_extract_json_array_raw():
    text = 'Prefix [{"account_name": "Beta"}] suffix'
    rows = extract_json_array_from_text(text)
    assert rows == [{"account_name": "Beta"}]


def test_extract_json_array_allows_null_entries_between_objects():
    text = '[{"account_name": "A"}, null, {"account_name": "B"}]'
    rows = extract_json_array_from_text(text)
    assert rows == [{"account_name": "A"}, {"account_name": "B"}]


def test_extract_json_array_bracket_inside_string_does_not_truncate():
    """Regression: naive ] depth counting breaks on brackets inside JSON string values."""
    text = """[
  {"account_name": "A", "priority_rank": 1, "notes": "See [0,1] and page ] footnotes"},
  {"account_name": "B", "priority_rank": 2, "notes": "plain"}
]"""
    rows = extract_json_array_from_text(text)
    assert rows == [
        {"account_name": "A", "priority_rank": 1, "notes": "See [0,1] and page ] footnotes"},
        {"account_name": "B", "priority_rank": 2, "notes": "plain"},
    ]


def test_records_from_structured_content_list():
    result = {"structuredContent": [{"account_name": "Co"}]}
    assert records_from_wisdom_tool_result(result) == [{"account_name": "Co"}]


def test_records_from_nested_structured_content():
    """Wisdom wraps structuredContent twice."""
    result = {
        "structuredContent": {
            "_meta": None,
            "content": [],
            "structuredContent": {
                "success": True,
                "results": [{"match_type": "x", "object_label": "Account"}],
            },
            "isError": False,
        }
    }
    rows = records_from_wisdom_tool_result(result)
    assert len(rows) == 1
    assert rows[0]["object_label"] == "Account"


def test_records_from_text_json():
    result = {
        "content": [
            {
                "type": "text",
                "text": '[{"account_name": "X", "competitor": "Statsig"}]',
            }
        ]
    }
    rows = records_from_wisdom_tool_result(result)
    assert rows == [{"account_name": "X", "competitor": "Statsig"}]


def test_records_success_false_returns_empty():
    result = {
        "structuredContent": {
            "success": False,
            "error": "ServiceError",
            "message": "upstream",
        }
    }
    assert records_from_wisdom_tool_result(result) == []


def test_records_enterpret_nested_iserror_wrapper_returns_empty():
    """Matches production: isError on wrapper, inner structuredContent has ServiceError."""
    result = {
        "structuredContent": {
            "_meta": None,
            "content": [],
            "structuredContent": {
                "success": False,
                "error": "",
                "error_type": "ServiceError",
                "details": {},
            },
            "isError": True,
        }
    }
    assert records_from_wisdom_tool_result(result) == []


def test_records_success_true_metadata_only_returns_empty():
    """search_knowledge_graph often echoes query without a results array."""
    result = {
        "structuredContent": {
            "_meta": None,
            "content": [],
            "structuredContent": {
                "success": True,
                "org_id": "f9b684d0-bf1d-4afa-895c-3b80a6f338f6",
                "query": "You guide Enterpret Wisdom queries…",
            },
            "isError": False,
        }
    }
    assert records_from_wisdom_tool_result(result) == []


def test_records_normalize_wrapped_entity():
    result = {
        "structuredContent": {
            "success": True,
            "results": [{"entity": {"account_name": "Acme", "urgency": "high"}}],
        }
    }
    rows = records_from_wisdom_tool_result(result)
    assert rows == [{"account_name": "Acme", "urgency": "high"}]


def test_records_from_entities_key():
    result = {
        "structuredContent": {
            "success": True,
            "entities": [{"label": "Contoso", "urgency": "watch"}],
        }
    }
    rows = records_from_wisdom_tool_result(result)
    assert rows == [{"label": "Contoso", "urgency": "watch"}]
