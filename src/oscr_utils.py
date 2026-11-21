"""Pure-Python helpers shared by the pipeline and unit tests. No pyspark or dbutils imports."""
import json
import math
import re

VALID_CALLS = {"rise", "hold", "fall"}


def parse_call(raw_text):
    """Return the first {...} JSON block with a valid call, or None."""
    if not isinstance(raw_text, str):
        return None
    for pattern in (r"\{.*?\}", r"\{.*\}"):
        match = re.search(pattern, raw_text, re.DOTALL)
        if not match:
            continue
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue
        call = str(obj.get("call", "")).strip().lower()
        if call in VALID_CALLS:
            obj["call"] = call
            return obj
    return None


def wilson_interval(hits, n, z=1.96):
    """Wilson score interval (lo, hi); same formula as 06_prediction_scorecard.sql. (None, None) when n == 0."""
    if n == 0:
        return None, None
    p = hits / n
    center = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    denom = 1 + z * z / n
    return (center - margin) / denom, (center + margin) / denom
