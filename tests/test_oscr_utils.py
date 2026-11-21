import pytest

from oscr_utils import VALID_CALLS, parse_call, wilson_interval


def test_clean_json_parsed_and_lowercased():
    out = parse_call('{"call": "RISE", "confidence": "high", "reasoning": "rank up"}')
    assert out["call"] == "rise"
    assert out["confidence"] == "high"


def test_json_wrapped_in_chatty_text():
    raw = 'Sure! Here is my answer:\n{"call": "hold", "confidence": "low"}\nHope that helps.'
    assert parse_call(raw)["call"] == "hold"


def test_invalid_call_returns_none():
    assert parse_call('{"call": "buy", "confidence": "high"}') is None


def test_non_string_returns_none():
    assert parse_call(None) is None


def test_valid_calls():
    assert VALID_CALLS == {"rise", "hold", "fall"}


def test_wilson_matches_scorecard():
    lo, hi = wilson_interval(27, 84)
    assert lo == pytest.approx(0.231, abs=0.002)
    assert hi == pytest.approx(0.427, abs=0.002)


def test_wilson_empty():
    assert wilson_interval(0, 0) == (None, None)
