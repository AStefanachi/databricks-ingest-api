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

# DBTITLE 1,List of json response per city
forecast_city_json = [
    WeatherAPI(API_KEY, city).get_forecast_weather() for city in CITIES
]

# COMMAND ----------

# DBTITLE 1,List of data frames per json response
forecasts_cities = [
    (
    # city_df    
    spark.createDataFrame(
        pd.json_normalize(
            response['location']
            )
        ),
    # astro information (sunset, sunrise)
    spark.createDataFrame(
        pd.json_normalize(
            response['forecast']['forecastday'][0]['astro']
            )
        )    
    )
    for response in forecast_city_json
]

# COMMAND ----------

# DBTITLE 1,List of cross join of city and astro information
cities_astro = [
    forecast[0].crossJoin(forecast[1]) for forecast in forecasts_cities
]

# COMMAND ----------

# DBTITLE 1,Reduce cities_astro into a single cities_df
cities_df = reduce(lambda acc, curr: acc.union(curr), cities_astro)

# COMMAND ----------

# DBTITLE 1,Adding the run_metadata() information via a crossJoin to the dataframes
metadata_df = pd.DataFrame([run_metadata()])
metadata_df = spark.createDataFrame(metadata_df)
cities_df = cities_df.crossJoin(metadata_df)

# COMMAND ----------

# DBTITLE 1,Check if the schema exists, otherwise create
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_weather")

# COMMAND ----------

# DBTITLE 1, Appending cities_df to bronze_weather.cities_astro
cities_df.write.mode("append").saveAsTable("bronze_weather.cities_astro")