import csv
import logging

from airflow.models import DAG
# Newer version will use this import, but old import still work
# previously: from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy import DummyOperator
# Previously: from airflow.operator.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.sensors.http import HttpSensor

import requests


PROJECT_PATH = '/Users/yothinix/projects/_class/my-airflow'

# Function name in Task mostly prefix with underscore since
# it mostly named same as tasks name
def _download_covid19_data(**kwargs):
    # print(kwargs)
    ds = kwargs['ds']
    yesterday_ds = kwargs['yesterday_ds']

    domain = 'https://api.covid19api.com'
    url = f'{domain}/world?from={yesterday_ds}T00:00:00Z&to={ds}T00:00:00Z'
    response = requests.get(url)
    data = response.json()

    file_path = f'{PROJECT_PATH}/covid19-{ds}.csv'
    with open(file_path, 'w') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([data[0]['NewConfirmed']])
        logging.info('Save COVID-19 data to CSV file successfully')

    return data


def _load_data_to_s3(**kwargs):
    ds = kwargs['ds']
    hook = S3Hook(aws_conn_id='aws_s3_conn')
    hook.load_file(
        filename=f'{PROJECT_PATH}/covid19-{ds}.csv',
        key=f'human/covid19-{ds}.csv',
        bucket_name='odds-dataops',
        replace=True
    )


# Name the DAG
# schedule_interval: Define when to run @daily == 0 0 * * *
# Start date: Beware that Python's DateTime not including timezone, DST

default_args = {
    'owner': 'human',
    'email': ['yothinix@odds.team',]
}
with DAG('covid19_data_processing',
         schedule_interval='@daily',
         default_args=default_args, # Dont forget to add default_args here
         start_date=timezone.datetime(2021, 3, 1),
         tags=['covid19', 'ODDS']) as dag:

    # Defining Operator task
    start = DummyOperator(task_id='start')

    print_prev_ds = BashOperator(
        task_id='print_prev_ds',
        bash_command='echo {{ prev_ds }} {{ macros.ds_add("2015-01-01", 5) }}'
    )

    check_api = HttpSensor(
        task_id='check_api',
        endpoint='/world',
        response_check=lambda response: response.status_code == 200
    )

    download_covid19_data = PythonOperator(
        task_id='download_covid19_data',
        python_callable=_download_covid19_data
    )

    create_table = SqliteOperator(
        task_id='create_table',
        sqlite_conn_id='sqlite_default',
        sql='''
            CREATE TABLE IF NOT EXISTS covid19 (
                NewConfirmed TEXT NOT NULL
            );
        '''
    )

    load_data_to_db = BashOperator(
        task_id='load_data_to_db',
        bash_command='''
        sqlite3 -separator "," /Users/yothinix/projects/_class/my-airflow/covid19.db ".import /Users/yothinix/projects/_class/my-airflow/covid19-{{ ds }}.csv covid19"
        '''
    )
    # if we have f-string we need to use 4: {{{{ XXX }}}} but template will use just 2 {{ ds }}

    send_email = EmailOperator(
        task_id='send_email',
        to=['yothinix@odds.team',],
        subject='Finished loading COVID-19 data to db!',
        html_content='<h1 style="color: red">Yeah!</h1>'
    )

    load_data_to_s3 = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=_load_data_to_s3
    )

    end = DummyOperator(task_id='end')

    # Defining how task ordered
    start >> print_prev_ds \
        >> check_api \
        >> download_covid19_data \
        >> create_table \
        >> load_data_to_db \
        >> send_email \
        >> end
    download_covid19_data >> load_data_to_s3

