from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import requests

def get_item_url_by_id(id):
    log.info(f"id is {id}")
    return f"https://hacker-news.firebaseio.com/v0/item/{id}.json"

def load_max_item_dict():
    max_item_url = "https://hacker-news.firebaseio.com/v0/maxitem.json"
    maxitem = requests.get(max_item_url)
    response = requests.get(get_item_url_by_id(maxitem))
    max_item_dict = response.json()
    log.info(f"Max item is {max_item_dict}")
    return max_item_dict

with DAG(
    dag_id="My_Second_CS_280_DAG",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 1, 20, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_max_item = PythonOperator(task_id="load_max_item", python_callable=load_max_item_dict)
    end_task = DummyOperator(task_id="end_task")

load_max_item >> end_task