import time
import json
import requests
import pandas as pd
from resources.api_key import api_key



def FetchOpenWeather(city):
    """
    Fetch the json response from OpenWeather API.
    Response consists of cloudiness, country, date,
    humidity, lat, lng, max temp, wind speed, etc.
    """
    
    url = "http://api.openweathermap.org/data/2.5/weather?"
    # Create a list for weather data
    city_data = []

    # Build partial query URL and send request
    query_url = f"{url}appid={api_key}&units=metric&q={city}"
    response = requests.get(query_url).json()
    
    # Extract required weather information from response
    current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    current_weather = response["weather"][0]["main"]
    cloudiness = response["clouds"]["all"]
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(response["dt"]))
    humidity = response["main"]["humidity"]
    pressure = response["main"]["pressure"]
    current_temp = response["main"]["temp"]
    min_temp = response["main"]["temp_min"]
    max_temp = response["main"]["temp_max"]
    wind_speed = response["wind"]["speed"]
    wind_degree = response["wind"]["deg"]
    visibility = response["visibility"]

    # Append the data into city_data
    city_data.append({
        "Fetch Time":   current_time,
        "Date":         dt,
        "City":         city,
        "Weather":      current_weather,
        "Temp":         current_temp,
        "Min Temp":     min_temp,
        "Max Temp":     max_temp,
        "Cloudiness":   cloudiness,
        "Pressure":     pressure,
        "Humidity":     humidity,
        "Wind Speed":   wind_speed,
        "Wind Degree":  wind_degree,
        "Visibility":   visibility
    })

    return city_data

def UpdateCsvFile(city):
    """
    This function updates the csv file in export folder,
    if the file does not exist, it will create one
    """

    # Get result from API
    city_data = FetchOpenWeather(city)
    city_data_df = pd.DataFrame(city_data)
    
    # Create or open csv file then append the weather info
    with open("weather.csv", 'a') as f:
        city_data_df.to_csv(f, index=False, header=f.tell()==0)
    
    print(city_data_df)

def PrintMsg():
    """
    Print out a msg in log file to finishe the DAG
    """
    print("The weather information has been retrieved.")