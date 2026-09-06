# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bootcamp_students.oscr_gold.app_rank_history (
# MAGIC   id STRING,
# MAGIC   _storefront STRING,
# MAGIC   name STRING,
# MAGIC   rank INT,
# MAGIC   snapshot_date DATE
# MAGIC ) USING DELTA;