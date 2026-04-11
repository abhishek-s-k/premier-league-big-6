# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W

# COMMAND ----------

# pl_big_6.bronze.match_data
df_match_data = spark.read.table("pl_big_6.bronze.transfer_data")
# .where(F.to_date(F.col('created_at'))>=F.current_date())

# COMMAND ----------

display(df_match_data.count())

# COMMAND ----------

# display(df_match_data)
df_match_stats = df_match_data.select(
    F.coalesce(F.try_to_date(F.col("Date"),'dd/MM/yyyy'),F.try_to_date(F.col("Date"),'dd/MM/yy')).alias('Date'),
    F.col("HomeTeam"),
    F.col("AwayTeam"),
    F.col("FTHG").cast('int'),
    F.col("FTAG").cast('int'),
    F.col("FTR")
).distinct()


# COMMAND ----------

df_teams_table_home = df_match_stats.groupBy(F.col("HomeTeam").alias("Team")).agg(
    F.sum(F.col("FTHG")).alias("GH"),
    F.sum(F.col("FTAG")).alias("GCH"),
    F.count(F.lit(1)).alias("MP"),
    F.sum(
        F.when(F.col("FTR") == "D", 1).when(F.col("FTR") == "A", 0).otherwise(3)
    ).alias("PointsHome"),
)

# COMMAND ----------

df_teams_table_away = df_match_stats.groupBy(F.col("AwayTeam").alias("Team")).agg(
    F.sum(F.col("FTAG")).alias("GA"),
    F.sum(F.col("FTHG")).alias("GCA"),
    F.count(F.lit(1)).alias("MP"),
    F.sum(
        F.when(F.col("FTR") == "D", 1).when(F.col("FTR") == "H", 0).otherwise(3)
    ).alias("PointsAway"),
)
df_teams_table = (
    df_teams_table_home.join(
        df_teams_table_away,
        df_teams_table_home.Team == df_teams_table_away.Team,
        "LEFT",
    )
    .withColumn("Goals", F.col("GH") + F.col("GA"))
    .withColumn("GoalsConceded", F.col("GCH") + F.col("GCA"))
    .withColumn("Points", F.col("PointsHome") + F.col("PointsAway"))
).select(
    df_teams_table_home.Team,
    (df_teams_table_home.MP + df_teams_table_away.MP).alias("MP"),
    "Points",
    "PointsHome",
    "PointsAway",
    (F.col("Goals") - F.col("GoalsConceded")).alias("GD"),
    "Goals",
    "GH",
    "GA",
    "GoalsConceded",
    "GCH",
    "GCA"
)

# COMMAND ----------

df_teams_table.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('pl_big_6.silver.match_stats')
