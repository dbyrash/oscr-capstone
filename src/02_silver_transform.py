# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col 


bronze_df = spark.table("bootcamp_students.oscr_bronze.finnhub_recommendation_trends")

window = Window.partitionBy("symbol", "period").orderBy(col("_ingested_at").desc())

silver_df = (
    bronze_df
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_silver.finnhub_recommendation_trends")

spark.table("bootcamp_students.oscr_silver.finnhub_recommendation_trends").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, row_number, to_date, from_unixtime

bronze_df = spark.table("bootcamp_students.oscr_bronze.finnhub_company_news") 
# bronze_df.show() 

window = Window.partitionBy("id").orderBy(col("_ingested_at").desc())

silver_df = (bronze_df
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn", "category", "image")
    .withColumn("news_date", to_date(from_unixtime(col("datetime"))))
    .drop("datetime") 
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_silver.finnhub_company_news")
                                                              
spark.table("bootcamp_students.oscr_silver.finnhub_company_news").show()



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, to_date

bronze_df = spark.table("bootcamp_students.oscr_bronze.fmp_analyst_estimates")

window = Window.partitionBy("symbol", "date").orderBy(col("_ingested_at").desc())

silver_df = (
    bronze_df
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("date", to_date(col("date")))
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_silver.fmp_analyst_estimates")
spark.table("bootcamp_students.oscr_silver.fmp_analyst_estimates").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

bronze_df = spark.table("bootcamp_students.oscr_bronze.tmdb_trending_shows")

window = Window.partitionBy("show_id", "platform").orderBy(col("_ingested_at").desc())

silver_df = (
    bronze_df
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_silver.tmdb_trending_shows")
spark.table("bootcamp_students.oscr_silver.tmdb_trending_shows").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

reviews_bronze = spark.table("bootcamp_students.oscr_bronze.tmdb_reviews")

window = Window.partitionBy("id").orderBy(col("_ingested_at").desc())

reviews_deduped = (
    reviews_bronze
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

show_names = (
    spark.table("bootcamp_students.oscr_silver.tmdb_trending_shows")
    .select("show_id", "name")
    .distinct()
)

silver_df = reviews_deduped.join(show_names, reviews_deduped["_show_id"] == show_names["show_id"], "left").drop("show_id")

silver_df.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_silver.tmdb_reviews")
spark.table("bootcamp_students.oscr_silver.tmdb_reviews").show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col


bronze_df = spark.table("bootcamp_students.oscr_bronze.tmdb_watch_providers")

window = Window.partitionBy("provider_id").orderBy(col("_ingested_at").desc())

silver_df = (
    bronze_df
    .withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

silver_df.write.format("delta").mode("overwrite").saveAsTable("bootcamp_students.oscr_silver.tmdb_watch_providers")
spark.table("bootcamp_students.oscr_silver.tmdb_watch_providers").show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, to_date, max as spark_max
from delta.tables import DeltaTable

bronze_df = spark.table("bootcamp_students.oscr_bronze.appstore_top_free")

latest_date = bronze_df.select(spark_max(to_date("_ingested_at"))).collect()[0][0]
bronze_today = bronze_df.filter(to_date(col("_ingested_at")) == latest_date)

window = Window.partitionBy("id", "_storefront").orderBy(col("_ingested_at").desc())
deduped = (
    bronze_today.withColumn("_rn", row_number().over(window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

brand_aliases = spark.createDataFrame(
    [
        ("Netflix", "Netflix"),
        ("Disney+", "Disney+"),
        ("Disney+", "Disney Plus"),
        ("Disney+", "Hotstar"),
        ("Disney+", "JioHotstar"),
    ],
    ["brand", "name"],
)

silver_df = deduped.join(brand_aliases, on="name", how="left")

silver_tbl = DeltaTable.forName(spark, "bootcamp_students.oscr_silver.appstore_top_free")
silver_tbl.alias("t").merge(
    silver_df.alias("s"),
    "t.id = s.id AND t._storefront = s._storefront"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

spark.table("bootcamp_students.oscr_silver.appstore_top_free").filter(col("brand").isNotNull()).show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bootcamp_students.oscr_gold.rag_text_corpus AS
# MAGIC SELECT
# MAGIC   CONCAT('news_', id) AS doc_id,
# MAGIC   'news' AS source_type,
# MAGIC   related AS entity,
# MAGIC   CONCAT(headline, '. ', summary) AS text_content,
# MAGIC   news_date AS published_date
# MAGIC FROM bootcamp_students.oscr_silver.finnhub_company_news
# MAGIC WHERE related IN ('NFLX', 'DIS')
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   CONCAT('review_', r._show_id, '_', r.id) AS doc_id,
# MAGIC   'review' AS source_type,
# MAGIC   CAST(r._show_id AS STRING) AS entity,
# MAGIC   r.content AS text_content,
# MAGIC   CAST(r.created_at AS DATE) AS published_date
# MAGIC FROM bootcamp_students.oscr_silver.tmdb_reviews r
# MAGIC WHERE r.content IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM bootcamp_students.oscr_gold.rag_text_corpus) AS corpus_total,
# MAGIC   (SELECT COUNT(*) FROM bootcamp_students.oscr_silver.finnhub_company_news WHERE related IN ('NFLX', 'DIS')) AS news_matching,
# MAGIC   (SELECT COUNT(*) FROM bootcamp_students.oscr_silver.tmdb_reviews WHERE content IS NOT NULL) AS reviews_matching

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bootcamp_students.oscr_gold.rag_text_corpus SET TBLPROPERTIES (delta.enableChangeDataFeed = true)