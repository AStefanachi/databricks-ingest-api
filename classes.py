# Databricks notebook source
# DBTITLE 1,Imports
import requests

# COMMAND ----------

# DBTITLE 1,Classes
class WeatherAPI:
    def __init__(self, api_key, city):
        """
        Initialize 3 variables:
        - api_key: secret API key provided by weatherapi.com
        - city: parameter that need to be passed in lowercase
        - API_URL: fixed value
        """
        self.api_key = api_key
        self.city = city
        self.API_URL = "http://api.weatherapi.com/v1/"
    
    def get_current_weather(self):
        """
        Passing the parameters to the current.json endpoint will return 
        json formatted information about the current weather
        """
        REQUEST_CURRENT_WEATHER = self.API_URL + "current.json"
        params = {
            "key": self.api_key,
            "q": self.city
        }
        response = requests.get(REQUEST_CURRENT_WEATHER, params=params)
        return response.json()
    
    def get_forecast_weather(self):
        """
        Passing the parameters to the forecast.json endpoint will return
        a json formatted information about the forecast for the city
        including as well sunrise and sunset information
        """
        REQUEST_FORECAST_WEATHER = self.API_URL + "forecast.json"
        params = {
            "key": self.api_key,
            "q": self.city
        }
        response = requests.get(REQUEST_FORECAST_WEATHER, params=params)
        return response.json()
