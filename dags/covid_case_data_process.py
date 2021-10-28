import logging
import requests

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.mongo.sensors.mongo import MongoSensor
from airflow.providers.mongo.hooks.mongo import MongoHook

default_args = {
    "Owner": "Joe Kim",
    "start_date": timezone.datetime(2021, 10, 27)
}


def _get_data(ds, ti) -> str:
    logging.info(ds)

    url = "https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all"
    response = requests.get(url)
    data = response.json()
    # for each in data:
    #     if each["txn_date"] == ds:
    #         latest_record = each
    #         break

    # ti.xcom_push(key="covid_data", value=latest_record)
    ti.xcom_push(key="covid_data", value=data)


def _insert_data(ds, ti):
    mongo = MongoHook(conn_id="mongodb_connection")
    data = ti.xcom_pull(task_ids="get_data", key="covid_data")
    logging.info(ds)
    logging.info(type(data))
    mongo.insert_many(mongo_db="covid_case", mongo_collection="daily", docs=data)


with DAG(
    "covid_case_data_process",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    tags=["covid_data"],
    max_active_runs=3,
) as dag:

    start = DummyOperator(task_id="start")

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="",
        endpoint="https://covid19.ddc.moph.go.th/api/Cases/timeline-cases-all",
        poke_interval=5,
        timeout=100,
    )

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
    )

    check_db_connection = MongoSensor(
        collection="healthcheck",
        query={"checked": True},
        mongo_conn_id="mongodb_connection",
        task_id="check_db_connection"
    )

    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=_insert_data,
    )

    end = DummyOperator(task_id="end")

    start >> check_api >> check_db_connection >> get_data >> insert_data >> end
