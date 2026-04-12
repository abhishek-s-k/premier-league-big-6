# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W

# COMMAND ----------

df_match_data = (spark.read.load("/Volumes/pl_big_6/silver/pl_data/match_data").where(F.to_date(F.col('created_at'))>=F.current_date()))
# .where(F.col('session') == F.when(F.month(F.current_date())>=7,F.concat(F.year(F.current_date()),F.lit('-'),F.year(F.current_date())+1)).otherwise(F.concat(F.year(F.current_date())-1,F.lit('-'),F.year(F.current_date()))))
#explore chnage data feed

# COMMAND ----------

df_match_data.count()

# COMMAND ----------

df_teams_table_home = df_match_data.groupBy(F.col('season'),F.col("HomeTeam").alias("Team")).agg(
    F.sum(F.col("FTHG")).alias("GH"),
    F.sum(F.col("FTAG")).alias("GCH"),
    F.count(F.lit(1)).alias("MP"),
    F.sum(
        F.when(F.col("FTR") == "D", 1).when(F.col("FTR") == "A", 0).otherwise(3)
    ).alias("PointsHome"),
)
df_teams_table_home.display()

# COMMAND ----------

df_teams_table_away = df_match_data.groupBy(F.col('season'),F.col("AwayTeam").alias("Team")).agg(
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
    df_teams_table_home.season,
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

seasons = df_teams_table.selectExpr("season").distinct().collect()
list_of_seasons= [row[0] for row in seasons]
# for i in seasons:
#     list_of_seasons.append(i)
condition = 'season in ('+",".join(f"'{i}'" for i in list_of_seasons)+')'
print(condition)

# COMMAND ----------

(df_teams_table.write.format('delta')
    # .partitionBy("season")
    .mode('overwrite')
    .option("replaceWhere",condition)
    # .save('/Volumes/pl_big_6/gold/match_data/pl_match_data')
    .saveAsTable('pl_big_6.gold.match_data')
)
