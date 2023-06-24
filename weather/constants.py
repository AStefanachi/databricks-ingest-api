# Databricks notebook source
# DBTITLE 1,Constants
SECRET_SCOPE = "kwdwe001"

# List of cities
CITIES = ["hamburg", "berlin", "bremen", "frankfurt"]

# Api key from weatherapi.com
API_KEY = dbutils.secrets.get(SECRET_SCOPE, "weather-api-key")