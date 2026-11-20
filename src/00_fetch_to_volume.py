# Fetches Finnhub, FMP, TMDB and App Store data into the raw landing volume.
import os
import json
import time
 
from pathlib import Path
from datetime import datetime, date, timedelta, timezone

import requests
import pandas as pd
 
# __file__ is missing in the Databricks editor; fall back to cwd.
try:
    _env_dir = Path(__file__).resolve().parent
except NameError:
    _env_dir = Path(os.getcwd())

try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=_env_dir / ".env")
except ImportError:
    pass

SECRET_SCOPE = "oscr"


def get_secret(name):
    """Databricks secret scope first; env var or .env when running locally."""
    try:
        from databricks.sdk.runtime import dbutils
        return dbutils.secrets.get(SECRET_SCOPE, name)
    except Exception:
        return os.getenv(name)


FINNHUB_API_KEY = get_secret("FINNHUB_API_KEY")
TMDB_API_KEY = get_secret("TMDB_API_KEY")
FMP_API_KEY = get_secret("FMP_API_KEY")
 
# API URLs
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
TMDB_BASE_URL = "https://api.themoviedb.org/3"
FMP_BASE_URL = "https://financialmodelingprep.com/stable"


VOLUME_PATH = "/Volumes/bootcamp_students/oscr_bronze/raw_landing"
OUTPUT_DIR = Path(f"{VOLUME_PATH}/raw_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
 
RETRIES = 2
BACKOFF_SECONDS = 2

# (label, status, ok, required) for every call; drives the end-of-run summary.
RESULTS = []


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def http_get(url, params=None):
    """GET with RETRIES retries and a growing backoff; returns the last response, or None."""
    r = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r
        except requests.RequestException as e:
            # Exception text can include the URL and its token, so log the type only.
            print(f"  request error: {type(e).__name__}")
            r = None
        if attempt < RETRIES:
            time.sleep(BACKOFF_SECONDS * (attempt + 1))
    return r


def with_fetched_at(data, fetched_at=None):
    """Stamp each record (list files) or the top level (dict files) with _fetched_at."""
    fetched_at = fetched_at or now_utc()
    if isinstance(data, list):
        return [{**d, "_fetched_at": fetched_at} if isinstance(d, dict) else d for d in data]
    if isinstance(data, dict):
        return {**data, "_fetched_at": fetched_at}
    return data


def save_json(name, data):
    # Write a temp file and swap it in, so a crash never leaves a half-written file.
    path = OUTPUT_DIR / f"{name}.json"
    tmp = OUTPUT_DIR / f"{name}.json.tmp"
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)
    print(f"saved {path}")
    preview(name, data)


def record(label, r, name=None, required=True):
    """Log one call; save its body only on 200 with a non-empty body. Returns the body or None."""
    status = r.status_code if r is not None else "ERR"
    body = None
    if status == 200:
        try:
            body = r.json()
        except ValueError:
            body = None
    ok = status == 200 and bool(body)
    shown = "200 empty" if status == 200 and not ok else status
    print(f"{label}: {shown}")
    if r is not None and status != 200:
        print(f"  {r.text[:200]}")
    if ok and name:
        save_json(name, with_fetched_at(body))
    RESULTS.append((label, shown, ok, required))
    return body if ok else None


def summarize():
    failed = [(l, s) for l, s, ok, req in RESULTS if req and not ok]
    warned = [(l, s) for l, s, ok, req in RESULTS if not req and not ok]
    print(f"---- summary: {len(RESULTS)} calls, {len(failed)} required failed, {len(warned)} optional failed ----")
    for l, s in warned:
        print(f"WARNING optional call failed: {l} -> {s}")
    for l, s in failed:
        print(f"FAILED required call: {l} -> {s}")
    if failed:
        raise RuntimeError(f"{len(failed)} required API call(s) failed: " + ", ".join(l for l, _ in failed))
 
def preview(name, data):
    try:
        if isinstance(data, list):
            df = pd.json_normalize(data) 
        elif isinstance(data, dict):
            list_field = next((v for v in data.values() if isinstance(v, list)), None) 
            df = pd.json_normalize(list_field) if list_field else pd.json_normalize([data]) 
        else:
            return 
        print(df.head()) 
    except Exception as e:
        print(f" (no tabular preview - shape doesn't flatten cleanly: {e})" ) 


def fetch_finnhub_news(symbol):
    today = date.today()
    week_ago = today - timedelta(days=7)
    return http_get(
        f"{FINNHUB_BASE_URL}/company-news",
        params={"symbol": symbol, "from": str(week_ago), "to": str(today), "token": FINNHUB_API_KEY},
    )


def check_finnhub_news_range(symbol, from_date, to_date):
    """One-off diagnostic: does a narrow, explicit date range return articles
    that a wide 365-day request silently dropped? Not part of the regular
    pipeline — call directly when investigating gaps in gold_symbol_analyst_sentiment.
    """
    r = http_get(
        f"{FINNHUB_BASE_URL}/company-news",
        params={"symbol": symbol, "from": from_date, "to": to_date, "token": FINNHUB_API_KEY},
    )
    articles = r.json() if r is not None and r.ok else []
    print(f"{symbol} {from_date} to {to_date}: {len(articles)} articles")
    return articles


def backfill_finnhub_news(symbol, days_back=365, chunk_days=30):
    """One-off historical backfill, chunked to avoid Finnhub silently
    truncating wide date-range requests — confirmed: a 365-day request
    returned 0 articles for May, but a narrow May-only request returned 247.
    Call directly, once. Not part of the daily pipeline (that stays on the
    cheap 7-day fetch_finnhub_news).
    """
    today = date.today()
    cutoff = today - timedelta(days=days_back)
    all_articles = []
    chunk_end = today
    while chunk_end > cutoff:
        chunk_start = max(chunk_end - timedelta(days=chunk_days), cutoff)
        r = http_get(
            f"{FINNHUB_BASE_URL}/company-news",
            params={"symbol": symbol, "from": str(chunk_start), "to": str(chunk_end), "token": FINNHUB_API_KEY},
        )
        if r is not None and r.ok:
            batch = r.json()
            print(f"{symbol} {chunk_start} to {chunk_end}: {len(batch)} articles")
            all_articles.extend(batch)
        else:
            print(f"{symbol} {chunk_start} to {chunk_end}: FAILED {r.status_code if r is not None else 'ERR'}")
        chunk_end = chunk_start - timedelta(days=1)
    if all_articles:
        save_json(f"finnhub_company_news_{symbol}", with_fetched_at(all_articles))
    return all_articles


def fetch_finnhub_recommendation(symbol):
    return http_get(
        f"{FINNHUB_BASE_URL}/stock/recommendation",
        params={"symbol": symbol, "token": FINNHUB_API_KEY},
    )


def fetch_fmp_estimates(symbol):
    return http_get(
        f"{FMP_BASE_URL}/analyst-estimates",
        params={"symbol": symbol, "period": "annual", "apikey": FMP_API_KEY},
    )


def explore_symbol(symbol):
    print(f"---- {symbol} ----")

    record(f"company-news ({symbol})", fetch_finnhub_news(symbol), f"finnhub_company_news_{symbol}")
    record(f"recommendation ({symbol})", fetch_finnhub_recommendation(symbol), f"finnhub_recommendation_trends_{symbol}")
    record(f"analyst-estimates ({symbol})", fetch_fmp_estimates(symbol), f"fmp_analyst_estimates_{symbol}")



def fetch_tmdb_trending_shows(provider_id):
    return http_get(
        f"{TMDB_BASE_URL}/discover/tv",
        params={
            "api_key": TMDB_API_KEY,
            "with_watch_providers": provider_id,
            "watch_region": "US",
            "sort_by": "popularity.desc",
        },
    )

def fetch_tmdb_show_reviews_by_id(show_id):
    return http_get(
        f"{TMDB_BASE_URL}/tv/{show_id}/reviews", params={"api_key": TMDB_API_KEY}
    )

def explore_tmdb_trending(limit=5):
    print("---- TMDB (dynamic discovery) ----")
    providers = {"netflix": 8, "disney_plus": 337}
    registry = []

    for platform, provider_id in providers.items():
        body = record(f"discover/tv ({platform})", fetch_tmdb_trending_shows(provider_id))
        for show in (body or {}).get("results", [])[:limit]:
            show_id, show_name = show["id"], show["name"]
            registry.append({"show_id": show_id, "name": show_name, "platform": platform})
            record(f"reviews for {show_name} ({show_id})", fetch_tmdb_show_reviews_by_id(show_id), f"tmdb_reviews_{show_id}")

    # An empty registry would wipe tmdb_trending_shows downstream, so never save one.
    if registry:
        save_json("tmdb_trending_show_registry", with_fetched_at(registry))
    RESULTS.append(("trending show registry", f"{len(registry)} shows", bool(registry), True))


def fetch_appstore_top_free(storefront):
    return http_get(f"https://rss.marketingtools.apple.com/api/v2/{storefront}/apps/top-free/100/apps.json")


def explore_appstore():
    print("---- App Store ----")
    for storefront in ["us", "de", "in"]:
        # Only the US chart feeds the popularity signal; other storefronts are optional.
        record(f"top-free/100 ({storefront})", fetch_appstore_top_free(storefront),
               f"appstore_top_free_100_{storefront}", required=(storefront == "us"))

def fetch_tmdb_watch_providers():
    return http_get(
        f"{TMDB_BASE_URL}/watch/providers/tv",
        params={"api_key": TMDB_API_KEY, "watch_region": "US"},
    )

def explore_tmdb_watch_providers():
    print("---- TMDB Watch Providers ----")
    record("watch/providers/tv", fetch_tmdb_watch_providers(), "tmdb_watch_providers")

if __name__ == "__main__":
    for key_name, key_value in [("FINNHUB_API_KEY", FINNHUB_API_KEY), ("FMP_API_KEY", FMP_API_KEY), ("TMDB_API_KEY", TMDB_API_KEY)]:
        print(f"{key_name}: {'set' if key_value else 'MISSING'}")
    # Backfill already ran; only the daily pull runs here.
    for symbol in ["NFLX", "DIS"]:
        explore_symbol(symbol)
    explore_tmdb_trending()
    explore_tmdb_watch_providers()
    explore_appstore()
    summarize()