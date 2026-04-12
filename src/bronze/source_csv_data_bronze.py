# Databricks notebook source
import json

# COMMAND ----------

# dbutils.widgets.text("source_data_list", "")
# source_data_list = json.loads(dbutils.widgets.get("source_data_list"))
source_path = '/Volumes/pl_big_6/raw/pl_data/raw_data/'
source_schema = '/Volumes/pl_big_6/raw/pl_data/schema/'
source_checkpoint = '/Volumes/pl_big_6/raw/pl_data/checkpoint/'
bronze_location =  '/Volumes/pl_big_6/bronze/'

# COMMAND ----------

from pyspark.sql.functions import *

source_data_list = ['match_data','transfer_data']

for source_data in source_data_list:
    {source_path+source_data}

    df = (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation",f"{source_schema+source_data}_schema")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            # .option("cloudFiles.inferColumnTypes",True)
            .load(f"{source_path+source_data}")
            .withColumn("filepath", col("_metadata.file_path"))
            .withColumn("created_at", current_timestamp())
        )
    
    (
        df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{source_checkpoint+source_data}_checkpoint")
            .trigger(availableNow=True)
            .start(f"{bronze_location+source_data}/")
            # .toTable(f"pl_big_6.bronze.{source_data}")
    )

# from threading import Thread
# from pyspark.sql.functions import col, current_timestamp

# def ingest_source(source_data):
#     df = (spark
#         .readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("cloudFiles.schemaLocation", f"{source_schema}{source_data}_schema")
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .option("cloudFiles.rescuedDataColumn", "_rescued_data")
#         .load(f"{source_path}{source_data}")
#         .withColumn("filepath", col("_metadata.file_path"))
#         .withColumn("created_at", current_timestamp())
#     )

#     query = (df.writeStream
#         .format("delta")
#         .outputMode("append")
#         .option("checkpointLocation", f"{source_checkpoint}{source_data}_checkpoint")
#         .trigger(availableNow=True)
#         .toTable(f"pl_big_6.bronze.{source_data}")
#     )

#     query.awaitTermination()  # wait for this stream to finish


# threads = []

# for source_data in source_data_list:
#     t = Thread(target=ingest_source, args=(source_data,))
#     t.start()
#     threads.append(t)

# # Wait for all threads to finish
# for t in threads:
#     t.join()
