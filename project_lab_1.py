from airflow import DAG
from airflow.models import Variable
from airflow.models import TaskInstance
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
import requests

def get_auth_header():
  bearer_token = Variable.get("TWITTER_BEARER_TOKEN", deserialize_json=True)
  return {"Authorization": f"Bearer {bearer_token}"}

user_id = "44196397"
api_url = f"https://api.twitter.com/2/users/{user_id}"
request = requests.get(api_url, headers=get_auth_header())
print(request)

def my_task_func(ti: TaskInstance, **kwargs):
  my_list = [1,2,3,4,5]
  ti.xcom_push("i_love_ds", my_list)

def my_task_func_2(ti: TaskInstance, **kwargs):
  my_list = ti.xcom_pull(task_id="my_dummy_task", key="i_love_ds")
  log.info(my_list)
  #Should log the list [1,2,3,4,5] to this task's log.

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    my_task = PythonOperator(
        task_id="my_dummy_task",
        python_callable=my_task_func,
        provide_context=True,
    )
    my_task_two = PythonOperator(
    task_id="my_dummy_task_2",
    python_callable=my_task_func_2,
    provide_context=True,
    )

my_task >> my_task_two
