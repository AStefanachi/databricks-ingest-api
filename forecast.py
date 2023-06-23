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
    # hourly forecasts
    spark.createDataFrame(
        pd.json_normalize(
            response['forecast']['forecastday'][0]['hour']
            )
        )
    )
    for response in forecast_city_json
]

# COMMAND ----------

# DBTITLE 1,List of join of city and forecast information
cities_forecast = [
    forecast[0].join(forecast[1], 
                    on=(F.to_date(
                        forecast[1].time
                        ) == F.to_date(
                            forecast[0].localtime
                            )), 
                    how='left').drop('region', 'lat', 'lon', 'localtime_epoch', 'localtime')
    for forecast in forecasts_cities    
]

# COMMAND ----------

# DBTITLE 1,Reduce cities_forecast into a single forecast_df
forecast_df = reduce(lambda acc, curr: acc.union(curr), cities_forecast)

# COMMAND ----------

# DBTITLE 1,Adding the run_metadata() information via a crossJoin to the dataframes
metadata_df = pd.DataFrame([run_metadata()])
metadata_df = spark.createDataFrame(metadata_df)
forecast_df = forecast_df.crossJoin(metadata_df)

# COMMAND ----------

# DBTITLE 1,Check if the schema exists, otherwise create
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_weather")

# COMMAND ----------

# DBTITLE 1, Appending forecast_df to bronze_weather.hourly_forecasts
forecast_df.write.mode("append").saveAsTable("bronze_weather.hourly_forecasts")
