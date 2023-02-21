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
from models.config import Session
from models.Users import User
from models.Tweets import Tweet
from models.UserTimeseries import UserTimeseries
from models.TweetTimeseries import TweetTimeseries
import datetime

def load_data_from_db(ti: TaskInstance, **kwargs):
    session = Session()

    users = session.query(User).all()
    ti.xcom_push("user_ids", [u.user_id for u in users])

    tweets = session.query(Tweet).all()
    ti.xcom_push("tweet_ids", [(t.tweet_id, t.user_id) for t in tweets])
    
    session.close()

def get_auth_header():
  bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
  return {"Authorization": f"Bearer {bearer_token}"}

def send_request(api_url):
  request = requests.get(api_url, headers=get_auth_header())
  return request.json()

def load_tweets(ids):
  return send_request(f"https://api.twitter.com/2/tweets?ids={','.join([str(i) for i in ids])}&tweet.fields=public_metrics,author_id,text,created_at")

def get_recent_tweets(userId, tweetIds):
    recent_tweets = send_request(f"https://api.twitter.com/2/users/{userId}/tweets?max_results=5")
    newest_id = recent_tweets['meta']['newest_id']
    if newest_id not in tweetIds:
        tweet_ids = [t['id'] for t in recent_tweets['data']]
        for id in tweet_ids:
            if id not in tweetIds:
                tweetIds.append(id)

    return tweetIds

def load_users(ids):
  return send_request(f"https://api.twitter.com/2/users?ids={','.join([str(i) for i in ids])}&user.fields=public_metrics,profile_image_url,username,description,id")

def get_twitter_api_data(ti: TaskInstance, **kwargs):
    user_ids = ti.xcom_pull(task_ids="load_data_task", key="user_ids")
    log.info(f"user ids: {user_ids}")
    users = load_users(user_ids)
    log.info(users)
    ti.xcom_push("users", users)

    tweet_to_user_ids = ti.xcom_pull(task_ids="load_data_task", key="tweet_ids")
    tweet_ids = []
    for user_id in user_ids:
        tweet_ids.extend(get_recent_tweets(user_id, [i[0] for i in tweet_to_user_ids if i[1] == user_id]))

    log.info(f"tweet ids: {tweet_ids}")
    tweets = load_tweets(tweet_ids)
    log.info(tweets)
    ti.xcom_push("tweets", tweets)

    return

def sort_data_into_df(data):
  df = pd.DataFrame(data)
  log.info(df.head())
  return df

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
  users = ti.xcom_pull(task_ids="call_api_task", key="users")
  user_data = users['data']
  users_df = sort_data_into_df({'user_id': [i['id'] for i in user_data], 'username': [i['username'] for i in user_data], 'name': [i['name'] for i in user_data], 'followers_count': [i['public_metrics']['followers_count'] for i in user_data], 'following_count': [i['public_metrics']['following_count'] for i in user_data], 'tweet_count': [i['public_metrics']['tweet_count'] for i in user_data], 'listed_count': [i['public_metrics']['listed_count'] for i in user_data]})
  tweets = ti.xcom_pull(task_ids="call_api_task", key="tweets")
  tweet_data = tweets['data']
  tweets_df = sort_data_into_df({'tweet_id': [i['id'] for i in tweet_data], 'user_id': [i['author_id'] for i in tweet_data], 'date': [i['created_at'] for i in tweet_data], 'text': [i['text'] for i in tweet_data],'retweet_count': [i['public_metrics']['retweet_count'] for i in tweet_data], 'like_count': [i['public_metrics']['like_count'] for i in tweet_data]})
  client = storage.Client()
  bucket = client.get_bucket("s-b-apache-airflow-cs280")
  bucket.blob("data/tweets.csv").upload_from_string(tweets_df.to_csv(index=False), "text/csv")
  bucket.blob("data/users.csv").upload_from_string(users_df.to_csv(index=False), "text/csv")

def push_data_to_databox(name, metric: int):
  client_token = Variable.get("DATABOX_TOKEN")
  client = Client(f"{client_token}")
  client.push(name, metric)

def store_data(ti: TaskInstance, **kwargs):
  session = Session()

  users = pd.read_csv('gs://s-b-apache-airflow-cs280/data/users.csv')

  user_ids = ti.xcom_pull(task_ids="load_data_task", key="user_ids")
  user_int_ids = [int(i) for i in user_ids]

  user_data = []
  for index, user in users.iterrows():
    if int(user['user_id']) not in user_int_ids:
      user_obj = User(
        user_id = user['user_id'],
        username = user['username'],
        name = user['name'],
        created_at = datetime.now()
      )
      user_data.append(user_obj)
    user_timeseries_obj = UserTimeseries(
      user_id = user['user_id'],
      followers_count = user['followers_count'],
      following_count = user['following_count'],
      tweet_count = user['tweet_count'],
      listed_count = user['listed_count'],
      date = datetime.now()
    )
    user_data.append(user_timeseries_obj)
  
  session.add_all(user_data)
  session.commit()

  tweets = pd.read_csv('gs://s-b-apache-airflow-cs280/data/tweets.csv')
  tweet_to_user_ids = ti.xcom_pull(task_ids="load_data_task", key="tweet_ids")
  known_tweet_ids = [int(i[0]) for i in tweet_to_user_ids]

  tweet_data = []
  for index, tweet in tweets.iterrows():
    if int(tweet['tweet_id']) not in known_tweet_ids:
      tweet_obj = Tweet(
        tweet_id = tweet['tweet_id'],
        user_id = tweet['user_id'],
        text = tweet['text'],
        created_at = datetime.now()
      )
      tweet_data.append(tweet_obj)
    tweet_timeseries_obj = TweetTimeseries(
      tweet_id = tweet['tweet_id'],
      retweet_count = tweet['retweet_count'],
      favorite_count = tweet['like_count'],
      date = tweet['date']
    )
    tweet_data.append(tweet_timeseries_obj)

  session.add_all(tweet_data)
  session.commit()

  session.close()

with DAG(
    dag_id="project_lab_2_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 2, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_data_task = PythonOperator(
        task_id="load_data_task",
        python_callable=load_data_from_db,
        provide_context=True
    )
    call_api_task = PythonOperator(
      task_id="call_api_task",
      python_callable=get_twitter_api_data,
      provide_context=True
    )
    transform_data_task = PythonOperator(
      task_id="transform_data_task",
      python_callable=transform_twitter_api_data_func,
      provide_context=True
    )
    write_data_task = PythonOperator(
      task_id="write_data_task",
      python_callable=store_data,
      provide_context=True
    )

load_data_task >> call_api_task >> transform_data_task >> write_data_task