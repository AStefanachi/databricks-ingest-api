# Databricks notebook source
# DBTITLE 1,Constants notebook
# MAGIC %run ./constants

# COMMAND ----------

# DBTITLE 1,Classes notebook
# MAGIC %run ./classes

# COMMAND ----------

# DBTITLE 1,Classes functions
# MAGIC %run ./functions

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
import pandas as pd
from functools import reduce

# COMMAND ----------

# DBTITLE 1,Normalize the json response into a list of cleaned dataframes
cities_current_weather = [
        replace_dots_in_columns(
            spark.createDataFrame(
                pd.json_normalize(
                    WeatherAPI(API_KEY, city).get_json_response("current.json")
                    )
                ) 
        )
    for city in CITIES
]

# COMMAND ----------

# DBTITLE 1,Reduce into a single dataframe
cities_df_union = reduce(lambda acc, curr: acc.union(curr), cities_current_weather)

# COMMAND ----------

# DBTITLE 1,Adding the run_metadata() information via a crossJoin to the cities_df_union dataframe
metadata_df = pd.DataFrame([run_metadata()])
metadata_df = spark.createDataFrame(metadata_df)
cities_df_union = cities_df_union.crossJoin(metadata_df)

# COMMAND ----------

# DBTITLE 1,Check if the schema exists, otherwise create
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_weather")

# COMMAND ----------

# DBTITLE 1,Create table in append mode to build history
cities_df_union.write.mode("append").saveAsTable("bronze_weather.current")