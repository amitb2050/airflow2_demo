import imp
from airflow.decorators import dag, task
import logging
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from app.loaders import load_market_data, merge_data
from app.enrichers import enrich_data
from app.common import send_email

logging.basicConfig(
    format="%(name)s-%(levelname)s-%(asctime)s-%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# DAG_ID = "demo_workflow"

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


@dag(default_args=default_args)
def my_dag_decorate():

    @task(
        task_id='load_market_data',
        provide_context=True,
        # op_kwargs={
        #     'data_path': '/some/input/data/path'
        # }
    )
    def fn_load_market_data(data_path: str):
        return load_market_data(data_path)
    task_load_market_data = fn_load_market_data(
        data_path='/some/input/data/path')

    stock_data_type = "stock"

    @task(
        task_id=f'enrich_{stock_data_type}',
        provide_context=True,
        # op_kwargs={'data_type': stock_data_type},
    )
    def fn_enrich_stock(data_type: str):
        return enrich_data(data_type)

    task_enrich_stock = fn_enrich_stock(data_type=stock_data_type)

    task_load_market_data >> task_enrich_stock


dag = my_dag_decorate()
