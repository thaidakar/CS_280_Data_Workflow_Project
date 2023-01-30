from airflow import DAG
from airflow.models import Variable
from airflow.models import TaskInstance
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from databox import Client
from google.cloud import storage

def get_auth_header():
  log.info("getting token")
  bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
  return {"Authorization": f"Bearer {bearer_token}"}

def send_request(api_url):
  request = requests.get(api_url, headers=get_auth_header())
  return request.json()

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

def sort_data_into_df(data):
  df = pd.DataFrame(data)
  log.info(df.head())
  return df

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
  users = ti.xcom_pull(task_ids="get_twitter_api_data", key="users")
  user_data = users['data']
  users_df = sort_data_into_df({'user_id': [i['id'] for i in user_data], 'username': [i['username'] for i in user_data], 'name': [i['name'] for i in user_data], 'followers_count': [i['public_metrics']['followers_count'] for i in user_data], 'following_count': [i['public_metrics']['following_count'] for i in user_data], 'tweet_count': [i['public_metrics']['tweet_count'] for i in user_data], 'listed_count': [i['public_metrics']['listed_count'] for i in user_data]})
  tweets = ti.xcom_pull(task_ids="get_twitter_api_data", key="tweets")
  tweet_data = tweets['data']
  tweets_df = sort_data_into_df({'tweet_id': [i['id'] for i in tweet_data], 'text': [i['text'] for i in tweet_data],'retweet_count': [i['public_metrics']['retweet_count'] for i in tweet_data], 'reply_count': [i['public_metrics']['reply_count'] for i in tweet_data], 'like_count': [i['public_metrics']['like_count'] for i in tweet_data], 'quote_count': [i['public_metrics']['quote_count'] for i in tweet_data], 'impression_count': [i['public_metrics']['impression_count'] for i in tweet_data]})
  client = storage.Client()
  bucket = client.get_bucket("s-b-apache-airflow-cs280")
  bucket.blob("data/tweets.csv").upload_from_string(tweets_df.to_csv(index=False), "text/csv")
  bucket.blob("data/users.csv").upload_from_string(users_df.to_csv(index=False), "text/csv")

def push_data_to_databox(name, metric: int):
  client_token = Variable.get("DATABOX_TOKEN")
  client = Client(f"{client_token}")
  client.push(name, metric)

def upload_databox_data(ti: TaskInstance, **kwargs):
  users = pd.read_csv('gs://s-b-apache-airflow-cs280/data/users.csv')

  for index, user in users.iterrows():
    name = user['name']
    push_data_to_databox(f'{name}_followers_count', user['followers_count'])
    push_data_to_databox(f'{name}_following_count', user['following_count'])
    push_data_to_databox(f'{name}_tweet_count', user['tweet_count'])
    push_data_to_databox(f'{name}_listed_count', user['listed_count'])

  tweets = pd.read_csv('gs://s-b-apache-airflow-cs280/data/tweets.csv')

  for index, tweet in tweets.iterrows():
    id = tweet['tweet_id']
    push_data_to_databox(f'{id}', tweet['reply_count'])
    push_data_to_databox(f'{id}', tweet['like_count'])
    push_data_to_databox(f'{id}', tweet['impression_count'])
    push_data_to_databox(f'{id}', tweet['retweet_count'])

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
    transform_twitter_api_data_task = PythonOperator(
      task_id="transform_twitter_api_data",
      python_callable=transform_twitter_api_data_func,
      provide_context=True
    )
    upload_databox_data_task = PythonOperator(
      task_id="upload_databox_data",
      python_callable=upload_databox_data,
      provide_context=True
    )

my_dummy_task >> my_dummy_task_two >> get_twitter_api_data_task >> transform_twitter_api_data_task >> upload_databox_data_task
