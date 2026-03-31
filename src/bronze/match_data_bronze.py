# Databricks notebook source
from pyspark.sql.functions import *
df = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", '/Volumes/pl_big_6/raw/pl_data/schema')
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        # .option("cloudFiles.inferColumnTypes",True)
        .load('/Volumes/pl_big_6/raw/pl_data/raw_data')
        .withColumn("filepath", col("_metadata.file_path"))
        .withColumn("created_at", current_timestamp())
    )

# COMMAND ----------

(
    df.writeStream
      .format("delta")
      .option("checkpointLocation", "/Volumes/pl_big_6/raw/pl_data/checkpoints/")
      .trigger(availableNow=True)
      .toTable("pl_big_6.bronze.match_data")
)
