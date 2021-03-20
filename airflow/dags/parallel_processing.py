import time
import random

from airflow.models import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator


def _sleep():
    time.sleep(random.randint(1, 10))


default_args = {
    'owner': 'human'
}
with DAG('parallel_processing',
         schedule_interval='*/10 * * * *',
         start_date=timezone.datetime(2021, 3, 20),
         default_args=default_args,
         tags=['ODDS', 'parallel'],
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='t1',
        python_callable=_sleep,
    )
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_sleep,
    )
    t3 = PythonOperator(
        task_id='t3',
        python_callable=_sleep,
    )
    t4 = PythonOperator(
        task_id='t4',
        python_callable=_sleep,
    )
    t5 = PythonOperator(
        task_id='t5',
        python_callable=_sleep,
    )
    t6 = PythonOperator(
        task_id='t6',
        python_callable=_sleep,
    )
    t7 = PythonOperator(
        task_id='t7',
        python_callable=_sleep,
    )

    t1 >> [t2, t3, t4, t5, t6] >> t7
