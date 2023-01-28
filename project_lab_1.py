from airflow import DAG
from airflow.models import Variable
from airflow.models import TaskInstance
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
import requests

def get_auth_header():
  log.info("getting token")
  bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
  return {"Authorization": f"Bearer {bearer_token}"}

def send_request(api_url):
  request = requests.get(api_url, headers=get_auth_header())
  return request.json()

# user_id = "44196397"
# api_url = f"https://api.twitter.com/2/users/{user_id}"
# request = requests.get(api_url, headers=get_auth_header())
# print(request)

def my_task_func(ti: TaskInstance, **kwargs):
  my_list = [1,2,3,4,5]
  ti.xcom_push("i_love_ds", my_list)
  return

def my_task_func_2(ti: TaskInstance, **kwargs):
  my_list = ti.xcom_pull(task_ids="my_dummy_task_1", key="i_love_ds")
  log.info(my_list)
  return
  #Should log the list [1,2,3,4,5] to this task's log.

def load_tweets(ids):
  return send_request(f"https://api.twitter.com/2/tweets?ids={','.join([str(i) for i in ids])}&tweet.fields=public_metrics,author_id,text")

def load_users(ids):
  return send_request(f"https://api.twitter.com/2/users?ids={','.join([str(i) for i in ids])}&user.fields=public_metrics,profile_image_url,username,description,id")

def get_twitter_api_data(ti: TaskInstance, **kwargs):
  tweet_ids = Variable.get("TWITTER_TWEET_IDS", deserialize_json=True)
  log.info(f"tweet ids: {tweet_ids}")
  tweets = load_tweets(tweet_ids)
  log.info(tweets)
  ti.xcom_push("tweets", tweets)
  user_ids = Variable.get("TWITTER_USER_IDS", deserialize_json=True)
  log.info(f"user ids: {user_ids}")
  users = load_users(user_ids)
  log.info(users)
  ti.xcom_push("users", users)
  return
  #Should log the list [1,2,3,4,5] to this task's log.

with DAG(
    dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    my_dummy_task = PythonOperator(
        task_id="my_dummy_task_1",
        python_callable=my_task_func,
        provide_context=True,
    )
    my_dummy_task_two = PythonOperator(
        task_id="my_dummy_task_2",
        python_callable=my_task_func_2,
        provide_context=True,
    )
    get_twitter_api_data_task = PythonOperator(
        task_id="get_twitter_api_data",
        python_callable=get_twitter_api_data,
        provide_context=True
    )

my_dummy_task >> my_dummy_task_two >> get_twitter_api_data_task
