# Checks that the Finnhub, FMP and TMDB API keys work, without printing them.
import os
import sys

import requests

SECRET_SCOPE = "oscr"


def get_secret(name):
    """Databricks secret scope first; env var or .env when running locally."""
    try:
        from databricks.sdk.runtime import dbutils
        return dbutils.secrets.get(SECRET_SCOPE, name)
    except Exception:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        return os.getenv(name)


# One cheap authenticated call per key; same endpoints and auth style as 00_fetch_to_volume.py.
CHECKS = {
    "FINNHUB_API_KEY": ("https://finnhub.io/api/v1/quote", lambda k: {"symbol": "NFLX", "token": k}),
    "FMP_API_KEY": ("https://financialmodelingprep.com/stable/analyst-estimates",
                    lambda k: {"symbol": "NFLX", "period": "annual", "apikey": k}),
    "TMDB_API_KEY": ("https://api.themoviedb.org/3/movie/popular", lambda k: {"api_key": k}),
}


def verify(name):
    key = get_secret(name)
    if not key:
        print(f"{name}: not set")
        return False
    url, params = CHECKS[name]
    try:
        r = requests.get(url, params=params(key), timeout=30)
    except requests.RequestException as e:
        # Exception text can include the URL and its key, so log the type only.
        print(f"{name}: request error {type(e).__name__}")
        return False
    print(f"{name}: {'valid' if r.ok else 'invalid'} (HTTP {r.status_code})")
    return r.ok


if __name__ == "__main__":
    results = [verify(name) for name in CHECKS]
    if not all(results):
        sys.exit("One or more API keys failed")
    print("All API keys are valid")
