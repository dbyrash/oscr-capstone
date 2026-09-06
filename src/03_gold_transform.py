# Databricks notebook source
from pyspark.sql.functions import col, to_date, trunc, date_format, count

recommendations = spark.table("bootcamp_students.oscr_silver.finnhub_recommendation_trends")

news = spark.table("bootcamp_students.oscr_silver.finnhub_company_news")

news_monthly = (
    news
    .withColumn("period", date_format(trunc(col("news_date"), "MM"), "yyyy-MM-01"))
    .filter(col("related").isin("NFLX", "DIS"))
    .groupBy(col("related").alias("symbol"), "period")
    .agg(count("*").alias("news_count"))
)

gold_df = (
    recommendations
    .withColumn(
        "mean_recommendation_score",
        (col("strongBuy") * 1 + col("buy") * 2 + col("hold") * 3 + col("sell") * 4 + col("strongSell") * 5)
        / (col("strongBuy") + col("buy") + col("hold") + col("sell") + col("strongSell"))
    )
    .join(news_monthly, on=["symbol", "period"], how="left")
    .fillna(0, subset=["news_count"])
)

gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bootcamp_students.oscr_gold.symbol_analyst_sentiment")
spark.table("bootcamp_students.oscr_gold.symbol_analyst_sentiment").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, round as spark_round 

estimates = spark.table("bootcamp_students.oscr_silver.fmp_analyst_estimates") 

symbol_window = Window.partitionBy("symbol").orderBy("date") 

gold_estimates = (
    estimates
    .withColumn("prior_revenueAvg", lag("revenueAvg").over(symbol_window))
    .withColumn("prior_epsAvg", lag("epsAvg").over(symbol_window))
    .withColumn(
        "revenue_yoy_growth_pct",
        spark_round((col("revenueAvg") - col("prior_revenueAvg")) / col("prior_revenueAvg") * 100, 2)
    )
    .withColumn(
        "eps_yoy_growth_pct",
        spark_round((col("epsAvg") - col("prior_epsAvg")) / col("prior_epsAvg") * 100, 2)
    )
    .drop("prior_revenueAvg", "prior_epsAvg")
)

gold_estimates.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bootcamp_students.oscr_gold.symbol_forward_estimates")

spark.table("bootcamp_students.oscr_gold.symbol_forward_estimates") \
    .select("symbol", "date", "revenueAvg", "revenue_yoy_growth_pct", "epsAvg", "eps_yoy_growth_pct", "numAnalystsRevenue", "numAnalystsEps") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import date_format, trunc, count, col

spark.table("bootcamp_students.oscr_silver.finnhub_company_news") \
    .withColumn("month", date_format(trunc(col("news_date"), "MM"), "yyyy-MM")) \
    .groupBy("related", "month") \
    .agg(count("*").alias("article_count")) \
    .orderBy("related", "month") \
    .show(50)

# COMMAND ----------


gold_app_rankings = (
    spark.table("bootcamp_students.oscr_silver.appstore_top_free")
    .filter(col("brand").isNotNull())
    .select("brand", "name", "_storefront", "_rank", to_date("_ingested_at").alias("snapshot_date"))
)

gold_app_rankings.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bootcamp_students.oscr_gold.app_rankings_by_brand")
spark.table("bootcamp_students.oscr_gold.app_rankings_by_brand").show()

# COMMAND ----------

from pyspark.sql.functions import count, avg, col

reviews = spark.table("bootcamp_students.oscr_silver.tmdb_reviews")
shows = spark.table("bootcamp_students.oscr_silver.tmdb_trending_shows").select("show_id", "name", "platform").distinct()

reviews_agg = (
    reviews
    .groupBy("_show_id")
    .agg(
        count("*").alias("review_count"),
        avg(col("author_details.rating")).alias("avg_rating")
    )
)

gold_show_engagement = (
    reviews_agg
    .join(shows, reviews_agg["_show_id"] == shows["show_id"], how="left")
    .drop("show_id") 
)

gold_show_engagement.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_gold.show_engagement")
spark.table("bootcamp_students.oscr_gold.show_engagement").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, to_date, max as spark_max, row_number
from pyspark.sql.window import Window

bronze_appstore = spark.table("bootcamp_students.oscr_bronze.appstore_top_free")
latest_date = bronze_appstore.select(spark_max(to_date("_ingested_at"))).collect()[0][0]

today_bronze = bronze_appstore.filter(to_date(col("_ingested_at")) == latest_date)

window = Window.partitionBy("id", "_storefront").orderBy(col("_ingested_at").desc())
today_snapshot = (
    today_bronze
        .withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1)
                .drop("_rn")
                    .withColumn("snapshot_date", to_date(col("_ingested_at")))
                        .select("id", "_storefront", "name", col("_rank").alias("rank"), "snapshot_date")
                        )
spark.sql(f"""
            DELETE FROM bootcamp_students.oscr_gold.app_rank_history
            WHERE snapshot_date = '{latest_date}'
        """)
today_snapshot.write.mode("append").saveAsTable("bootcamp_students.oscr_gold.app_rank_history")
today_snapshot.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_date, COUNT(*) AS row_count, COUNT(DISTINCT id, _storefront) AS distinct_apps
# MAGIC FROM bootcamp_students.oscr_gold.app_rank_history
# MAGIC GROUP BY snapshot_date
# MAGIC ORDER BY snapshot_date