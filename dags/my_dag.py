import codecs
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

import sys, os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from app.loaders import load_market_data, merge_data
from app.enrichers import enrich_data
from app.common import send_email

logging.basicConfig(
    format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DAG_ID = "demo_workflow"

default_args = {
    "owner": "Amit",
    "description": (
        "DAG to explain airflow concepts"
    ),
    "depends_on_past": False,
    "start_date": dates.days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "provide_context": True,
}

my_dag = DAG(
    DAG_ID,
    default_args=default_args,
    # schedule_interval=timedelta(minutes=5),
)


task_load_market_data = PythonOperator(
    task_id='load_market_data',
    python_callable=load_market_data,
    provide_context=True,
    op_kwargs={
        'data_path': '/some/input/data/path'
    },
    dag = my_dag
)

stock_data_type = "stock"
task_enrich_stock = PythonOperator(
    task_id=f'enrich_{stock_data_type}',
    python_callable=enrich_data,
    op_kwargs={'data_type': stock_data_type},
    provide_context=True,
    dag = my_dag
)

etf_data_type = "etf"
task_enrich_etf = PythonOperator(
    task_id=f'enrich_{etf_data_type}',
    python_callable=enrich_data,
    provide_context=True,
    op_kwargs={'data_type': etf_data_type},
    dag = my_dag
)

fx_data_type = "fx"
task_enrich_fx = PythonOperator(
    task_id=f'enrich_{fx_data_type}',
    python_callable=enrich_data,
    provide_context=True,
    op_kwargs={'data_type': fx_data_type},
    dag = my_dag
)

task_merge_data = PythonOperator(
    task_id=f'merge_data',
    python_callable=merge_data,
    provide_context=True,
    dag = my_dag,
    op_kwargs={'out_data_path': 'some/out/data/path',
               'data_types': [stock_data_type, etf_data_type, fx_data_type]},
)

task_notify = PythonOperator(
    task_id=f'send_email',
    python_callable=send_email,
    provide_context=True,
    op_kwargs={'to': ['ab@gmail.com', 'tb@gmail.com'],
               'subject': "ETL notification status"},
    dag = my_dag
)


task_load_market_data >> (task_enrich_stock, task_enrich_etf,
                          task_enrich_fx) >> task_merge_data >> task_notify
