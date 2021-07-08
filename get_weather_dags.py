# date: 2021-07-07
# summary: Getting Toronto Temperatures

import time
import json
import requests
import pandas as pd
from resources.api_key import api_key
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup

url = "http://api.openweathermap.org/data/2.5/weather?"
filename = "export/weather.csv"
cities = ['Toronto', 'Montreal']

def FetchOpenWeather(city):
    """
    Fetch the json response from OpenWeather API.
    Response consists of cloudiness, country, date,
    humidity, lat, lng, max temp, wind speed, etc.
    """

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

def UpdateCsvFile(city, filename):
    """
    This function updates the csv file in export folder,
    if the file does not exist, it will create one
    """

    # Get result from API
    city_data = FetchOpenWeather(city)
    city_data_df = pd.DataFrame(city_data)
    
    # Create or open csv file then append the weather info
    with open(filename, 'a') as f:
        city_data_df.to_csv(f, index=False, header=f.tell()==0)
    
    print(city_data_df)

# Default settings applied to all tasks
default_args = {
    'owner': 'kelvin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
} 

# Using a DAG context manager

with DAG('record_weather_dags',
         start_date=days_ago(1),
         max_active_runs=1,
         schedule_interval="@hourly",
         default_args=default_args,
         catchup=False,
         tags=['weather', 'toronto']
         ) as dag:

    # A dummy task to manage a group of tasks
    t0 = DummyOperator(task_id='start')

    # A task to send email
    send_email = EmailOperator(
        task_id='send_email',
        to=['k4tang@gmail.com'],
        subject='Saving Weather Data to CSV',
        html_content='<p>Fetching weather successfully. Files can now be found in export folder. <p>'
    )

    # Main task to fetch weather data
    with TaskGroup('weather_task_group') as weather_group:
        for city in cities:
            print(city)
            generate_files = PythonOperator(
                task_id='generate_file_{0}'.format(city),
                python_callable=UpdateCsvFile,
                op_kwargs={'city': city, 'filename': filename}
            )
    
    # weather_toronto = PythonOperator(
    #     task_id='weather_toronto',
    #     python_callable=UpdateCsvFile,
    #     op_kwargs={'city': 'Toronto', 'filename': filename},
    #     dag=dag,
    # )

    # weather_montreal = PythonOperator(
    #     task_id='weather_montreal',
    #     python_callable=UpdateCsvFile,
    #     op_kwargs={'city': 'Montreal', 'filename': filename},
    #     dag=dag,
    # )
    t0 >> weather_group >> send_email

    # t0 >> [weather_toronto, weather_montreal] >> send_email

# Testing locally 
# if __name__ == "__main__":
#     UpdateCsvFile(filename)