# date: 2021-07-07
# summary: Print simple msgs

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

def PrintMsg(msg):

    # Print msg
    print(msg)

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

with DAG('print_msg_test',
         start_date=days_ago(1),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=1),
         default_args=default_args,
         catchup=False,
         tags=['weather', 'toronto']
         ) as dag:

    # A dummy task to manage a group of tasks
    t0 = DummyOperator(task_id='start')

    # A task 
    first = PythonOperator(
        task_id='print_msg_1',
        python_callable=PrintMsg,
        op_kwargs={'msg': '11111'},
        dag=dag,
    )

    # A task 
    second = PythonOperator(
        task_id='print_msg_2',
        python_callable=PrintMsg,
        op_kwargs={'msg': '22222'},
        dag=dag,
    )
    
    # A task 
    third = PythonOperator(
        task_id='print_msg_3',
        python_callable=PrintMsg,
        op_kwargs={'msg': '33333'},
        dag=dag,
    )

    t0 >>  [third, first] >> second
