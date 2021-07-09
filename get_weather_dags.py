# date: 2021-07-07
# summary: Getting Toronto Temperatures

from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
# from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from weather_fetch import FetchOpenWeather, UpdateCsvFile, PrintMsg

cities = ['Toronto', 'Montreal']

# Default settings applied to all tasks
default_args = {
    'owner': 'kelvin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
} 

# Using a DAG context manager
with DAG('record_weather_dags',
         start_date=days_ago(1),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         catchup=False,
         tags=['weather', 'toronto']
         ) as dag:

    # A dummy task to manage a group of tasks
    t0 = DummyOperator(task_id='start')

    # A task to print msg
    print_msg = PythonOperator(
        task_id='print_msg',
        python_callable=PrintMsg,
        dag=dag,
    )

    # A task to send email
    # send_email = EmailOperator(
    #     task_id='send_email',
    #     to=['k4tang@gmail.com'],
    #     subject='Saving Weather Data to CSV',
    #     html_content='<p>Fetching weather successfully. Files can now be found in export folder. <p>'
    # )

    # Main task to fetch weather data
    with TaskGroup('weather_task_group') as weather_group:
        for city in cities:
            generate_files = PythonOperator(
                task_id='generate_file_{0}'.format(city),
                python_callable=UpdateCsvFile,
                op_kwargs={'city': city}
            )
    
    t0 >> weather_group >> print_msg