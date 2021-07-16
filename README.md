# weather_airflow_dags

An Airflow DAG which fetches weather data every 5 mins for Montreal and Toronto.

- Backfill used backfilling to re-do the missing tasks from the starting time.

- API: OpenweatherAPI

- Weather Data includes:

* "Fetch Time"
* "Date""City"
* "Weather"
* "Temp"
* "Min Temp"
* "Max Temp"
* "Cloudiness"
* "Pressure"
* "Humidity"
* "Wind Speed"
* "Wind Degree"
* "Visibility"y
