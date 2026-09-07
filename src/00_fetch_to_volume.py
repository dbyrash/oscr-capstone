import os
import json
 
from pathlib import Path
from datetime import datetime, date, timedelta

import requests
import pandas as pd
 
from dotenv import load_dotenv

# __file__ isn't defined when Databricks runs this via the web editor
# (it execs cell-by-cell, not `python script.py`), so fall back to cwd —
# which Databricks Repos already sets to this file's own folder.
try:
    _env_dir = Path(__file__).resolve().parent
except NameError:
    _env_dir = Path(os.getcwd())

load_dotenv(dotenv_path=_env_dir / ".env")

# Constants 
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")
 
# API URLs
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
POLYGON_BASE_URL = "https://api.polygon.io/v2"
TMDB_BASE_URL = "https://api.themoviedb.org/3"
FMP_BASE_URL = "https://financialmodelingprep.com/stable"


VOLUME_PATH = "/Volumes/bootcamp_students/oscr_bronze/raw_landing"
OUTPUT_DIR = Path(f"{VOLUME_PATH}/raw_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
 
def save_json(name, data):
    path = OUTPUT_DIR / f"{name}.json"
    path.write_text(json.dumps(data, indent=2)) 
    print(f"saved {path}")
    preview(name, data)
 
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
    return requests.get(
        f"{FINNHUB_BASE_URL}/company-news",
        params={"symbol": symbol, "from": str(week_ago), "to": str(today), "token": FINNHUB_API_KEY},
    )


def check_finnhub_news_range(symbol, from_date, to_date):
    """One-off diagnostic: does a narrow, explicit date range return articles
    that a wide 365-day request silently dropped? Not part of the regular
    pipeline — call directly when investigating gaps in gold_symbol_analyst_sentiment.
    """
    r = requests.get(
        f"{FINNHUB_BASE_URL}/company-news",
        params={"symbol": symbol, "from": from_date, "to": to_date, "token": FINNHUB_API_KEY},
    )
    articles = r.json()
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
        r = requests.get(
            f"{FINNHUB_BASE_URL}/company-news",
            params={"symbol": symbol, "from": str(chunk_start), "to": str(chunk_end), "token": FINNHUB_API_KEY},
        )
        if r.ok:
            batch = r.json()
            print(f"{symbol} {chunk_start} to {chunk_end}: {len(batch)} articles")
            all_articles.extend(batch)
        else:
            print(f"{symbol} {chunk_start} to {chunk_end}: FAILED {r.status_code} {r.text}")
        chunk_end = chunk_start - timedelta(days=1)
    save_json(f"finnhub_company_news_{symbol}", all_articles)
    return all_articles


def fetch_finnhub_recommendation(symbol):
    return requests.get(
        f"{FINNHUB_BASE_URL}/stock/recommendation",
        params={"symbol": symbol, "token": FINNHUB_API_KEY},
    )


def fetch_fmp_estimates(symbol):
    return requests.get(
        f"{FMP_BASE_URL}/analyst-estimates",
        params={"symbol": symbol, "period": "annual", "apikey": FMP_API_KEY},
    )


def explore_symbol(symbol):
    print(f"---- {symbol} ----")

    r = fetch_finnhub_news(symbol)
    print(f"company-news ({symbol}): {r.status_code}")
    save_json(f"finnhub_company_news_{symbol}", r.json()) if r.ok else print(r.text)

    r = fetch_finnhub_recommendation(symbol)
    print(f"recommendation ({symbol}): {r.status_code}")
    save_json(f"finnhub_recommendation_trends_{symbol}", r.json()) if r.ok else print(r.text)

    r = fetch_fmp_estimates(symbol)
    print(f"analyst-estimates ({symbol}): {r.status_code}")
    save_json(f"fmp_analyst_estimates_{symbol}", r.json()) if r.ok else print(r.text)



def fetch_tmdb_trending_shows(provider_id):
    return requests.get(
        f"{TMDB_BASE_URL}/discover/tv",
        params={
            "api_key": TMDB_API_KEY,
            "with_watch_providers": provider_id,
            "watch_region": "US",
            "sort_by": "popularity.desc",
        },
    )

def fetch_tmdb_show_reviews_by_id(show_id):
    return requests.get(
        f"{TMDB_BASE_URL}/tv/{show_id}/reviews", params={"api_key": TMDB_API_KEY}
    )

def explore_tmdb_trending(limit=5):
    print("---- TMDB (dynamic discovery) ----")
    providers = {"netflix": 8, "disney_plus": 337}
    registry = []

    for platform, provider_id in providers.items():
        r = fetch_tmdb_trending_shows(provider_id)
        print(f"discover/tv ({platform}): {r.status_code}")
        if not r.ok:
            print(r.text)
            continue
        for show in r.json().get("results", [])[:limit]:
            show_id, show_name = show["id"], show["name"]
            registry.append({"show_id": show_id, "name": show_name, "platform": platform})

            r_reviews = fetch_tmdb_show_reviews_by_id(show_id)
            print(f"reviews for {show_name} ({show_id}): {r_reviews.status_code}")
            save_json(f"tmdb_reviews_{show_id}", r_reviews.json()) if r_reviews.ok else print(r_reviews.text)

    save_json("tmdb_trending_show_registry", registry)


def fetch_appstore_top_free(storefront):
    return requests.get(f"https://rss.marketingtools.apple.com/api/v2/{storefront}/apps/top-free/100/apps.json")


def explore_appstore():
    print("---- App Store ----")
    for storefront in ["us", "de", "in"]:
        r = fetch_appstore_top_free(storefront)
        print(f"top-free/100 ({storefront}): {r.status_code}")
        save_json(f"appstore_top_free_100_{storefront}", r.json()) if r.ok else print(r.text)

def fetch_tmdb_watch_providers():
    return requests.get(
        f"{TMDB_BASE_URL}/watch/providers/tv",
        params={"api_key": TMDB_API_KEY, "watch_region": "US"},
    )

def explore_tmdb_watch_providers():
    print("---- TMDB Watch Providers ----")
    r = fetch_tmdb_watch_providers()
    print(f"watch/providers/tv: {r.status_code}")
    save_json("tmdb_watch_providers", r.json()) if r.ok else print(r.text)

if __name__ == "__main__":
    # Backfill already ran and landed successfully — don't call it again,
    # it would just overwrite itself with the 7-day window below it anyway.
    # Regular daily pipeline:
    for symbol in ["NFLX", "DIS"]:
        explore_symbol(symbol)
    explore_tmdb_trending()
    explore_tmdb_watch_providers()
    explore_appstore()