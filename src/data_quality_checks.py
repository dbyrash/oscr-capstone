import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def assert_row_count(table_name, min_rows=1):
    count = spark.table(table_name).count()
    assert count >= min_rows, f"{table_name}: expected at least {min_rows} rows, got {count}"
    print(f"[PASS] {table_name}: {count} rows")


def assert_no_nulls(table_name, columns):
    df = spark.table(table_name)
    for c in columns:
        null_count = df.filter(df[c].isNull()).count()
        assert null_count == 0, f"{table_name}.{c}: {null_count} unexpected nulls"
    print(f"[PASS] {table_name}: no nulls in {columns}")


def assert_no_duplicate_keys(table_name, key_columns):
    df = spark.table(table_name)
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    assert total == distinct, f"{table_name}: {total - distinct} duplicate rows on key {key_columns}"
    print(f"[PASS] {table_name}: no duplicates on {key_columns}")


FRESHNESS_HOURS = 36

# _fetched_at, not _ingested_at: bronze resets _ingested_at on every run even when the fetch failed.
FRESHNESS_SOURCES = [
    ("bootcamp_students.oscr_bronze.finnhub_company_news", None),
    ("bootcamp_students.oscr_bronze.finnhub_recommendation_trends", None),
    ("bootcamp_students.oscr_bronze.fmp_analyst_estimates", None),
    ("bootcamp_students.oscr_bronze.tmdb_reviews", None),
    ("bootcamp_students.oscr_bronze.tmdb_trending_shows", None),
    ("bootcamp_students.oscr_bronze.tmdb_watch_providers", None),
    ("bootcamp_students.oscr_bronze.appstore_top_free", "_storefront = 'us'"),
]


def assert_fresh(sources, max_hours=FRESHNESS_HOURS):
    """Check every source, then fail once listing all stale ones."""
    stale = []
    for table, where in sources:
        row = spark.sql(f"""
            SELECT CAST(MAX(_fetched_at) AS STRING) AS newest,
                   (unix_timestamp(current_timestamp()) - unix_timestamp(MAX(_fetched_at))) / 3600 AS age_h
            FROM {table} {'WHERE ' + where if where else ''}
        """).collect()[0]
        label = f"{table}{' [' + where + ']' if where else ''}"
        if row.newest is None or row.age_h > max_hours:
            age = "never" if row.newest is None else f"{row.age_h:.1f}h ago"
            stale.append(f"{label}: newest _fetched_at {row.newest} ({age})")
            print(f"[FAIL] {label}: newest _fetched_at {row.newest} ({age})")
        else:
            print(f"[PASS] {label}: newest _fetched_at {row.newest} ({row.age_h:.1f}h ago)")
    assert not stale, f"{len(stale)} source(s) older than {max_hours}h: " + "; ".join(stale)


def check_bronze():
    assert_row_count("bootcamp_students.oscr_bronze.finnhub_recommendation_trends")
    assert_no_nulls("bootcamp_students.oscr_bronze.finnhub_recommendation_trends", ["symbol", "period"])

    assert_row_count("bootcamp_students.oscr_bronze.appstore_top_free")
    assert_no_duplicate_keys("bootcamp_students.oscr_bronze.appstore_top_free", ["id", "_storefront", "_ingested_at"])

    assert_row_count("bootcamp_students.oscr_bronze.tmdb_trending_shows")
    assert_row_count("bootcamp_students.oscr_bronze.tmdb_reviews")

    assert_fresh(FRESHNESS_SOURCES)


def check_silver():
    assert_row_count("bootcamp_students.oscr_silver.finnhub_recommendation_trends")
    assert_no_duplicate_keys("bootcamp_students.oscr_silver.finnhub_recommendation_trends", ["symbol", "period"])

    assert_row_count("bootcamp_students.oscr_silver.appstore_top_free")
    assert_no_duplicate_keys("bootcamp_students.oscr_silver.appstore_top_free", ["id", "_storefront"])

    assert_row_count("bootcamp_students.oscr_silver.finnhub_company_news")
    assert_no_duplicate_keys("bootcamp_students.oscr_silver.finnhub_company_news", ["id"])


def check_gold():
    assert_row_count("bootcamp_students.oscr_gold.symbol_analyst_sentiment", min_rows=8)
    assert_no_nulls("bootcamp_students.oscr_gold.symbol_analyst_sentiment", ["mean_recommendation_score", "news_count"])

    assert_row_count("bootcamp_students.oscr_gold.symbol_forward_estimates")
    assert_no_nulls("bootcamp_students.oscr_gold.symbol_forward_estimates", ["revenueAvg", "epsAvg"])

    assert_row_count("bootcamp_students.oscr_gold.app_rankings_by_brand", min_rows=1)

    assert_row_count("bootcamp_students.oscr_gold.show_engagement", min_rows=1)
    assert_no_nulls("bootcamp_students.oscr_gold.show_engagement", ["review_count"])


PREDICTIONS = "bootcamp_students.oscr_gold.predictions"
PREDICTION_SKIPS = "bootcamp_students.oscr_gold.prediction_skips"
VALID_CALLS = ("rise", "hold", "fall")


def assert_calls_valid(table_name):
    allowed = ", ".join(f"'{c}'" for c in VALID_CALLS)
    bad = spark.sql(f"SELECT COUNT(*) AS n FROM {table_name} WHERE call NOT IN ({allowed})").first().n
    assert bad == 0, f"{table_name}: {bad} rows with call outside {VALID_CALLS}"
    print(f"[PASS] {table_name}: every call in {VALID_CALLS}")


def assert_skip_rate(days=7, max_rate=0.20):
    """Skips / (predictions + skips) by prediction_date over the last `days` days."""
    since = f"prediction_date >= date_sub(current_date(), {days})"
    preds = spark.sql(f"SELECT COUNT(*) AS n FROM {PREDICTIONS} WHERE {since}").first().n
    skips = spark.sql(f"SELECT COUNT(*) AS n FROM {PREDICTION_SKIPS} WHERE {since}").first().n
    total = preds + skips
    if total == 0:
        print(f"[SKIP] skip rate: no predictions or skips in the last {days} days")
        return
    rate = skips / total
    msg = f"skip rate last {days}d: {skips}/{total} = {rate:.1%} (limit < {max_rate:.0%})"
    if rate >= max_rate:
        print(f"[FAIL] {msg}")
    assert rate < max_rate, msg
    print(f"[PASS] {msg}")


def assert_no_call_collapse(days=14, min_rows=20, max_share=0.90):
    """Per variant, no single call may exceed max_share of live predictions; fail once listing all collapsed."""
    rows = spark.sql(f"""
        SELECT variant, call, COUNT(*) AS n,
               SUM(COUNT(*)) OVER (PARTITION BY variant) AS total
        FROM {PREDICTIONS}
        WHERE source = 'live' AND prediction_date >= date_sub(current_date(), {days})
        GROUP BY variant, call
    """).collect()
    top = {}
    for r in rows:
        if r.variant not in top or r.n > top[r.variant].n:
            top[r.variant] = r
    if not top:
        print(f"[SKIP] collapse check: not enough rows (0 live predictions in the last {days} days)")
    collapsed = []
    for variant, r in sorted(top.items()):
        if r.total < min_rows:
            print(f"[SKIP] collapse check {variant}: not enough rows ({r.total} < {min_rows})")
            continue
        share = r.n / r.total
        msg = f"collapse check {variant}: top call '{r.call}' {r.n}/{r.total} = {share:.1%} (limit {max_share:.0%})"
        if share > max_share:
            collapsed.append(msg)
            print(f"[FAIL] {msg}")
        else:
            print(f"[PASS] {msg}")
    assert not collapsed, f"{len(collapsed)} variant(s) collapsed: " + "; ".join(collapsed)


def check_predictions():
    assert_row_count(PREDICTIONS)
    assert_no_nulls(PREDICTIONS, ["symbol", "prediction_date", "source", "variant", "call"])
    assert_calls_valid(PREDICTIONS)
    assert_skip_rate()
    assert_no_call_collapse()


CHECKS = {"bronze": check_bronze, "silver": check_silver, "gold": check_gold, "predictions": check_predictions}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True, choices=list(CHECKS.keys()))
    args = parser.parse_args()

    print(f"---- Running {args.layer} checks ----")
    CHECKS[args.layer]()
    print(f"---- {args.layer} checks passed ----")
