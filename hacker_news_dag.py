import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago
# from python_function import  name_getter

import psycopg2
import requests
import datetime
import time
# Constants
BASE_URL = "https://hacker-news.firebaseio.com/v0/"
STORIES_URL = BASE_URL+'newstories.json'
ITEM_URL = BASE_URL+'item/'


def get_db_connection():
    try:
        conn = psycopg2.connect(
            database="hacker_new_db",
            user="hacker",
            password="HackerDB#33",
            host="host.docker.internal",
            port="5422"
        )
        return conn
    except Exception as e:
        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        print(e)
        return None


def execute_query(query):
    try:
        conn = get_db_connection()
        print(conn)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
        print(e)
        return None
    finally:
        conn.close()



def api_task():
    final_list = []

    # Fetch All Stories
    res = requests.get(STORIES_URL)
    stories = res.json()
    print(stories)

    for story_id in stories:
        # Fetch Item
        time.sleep(1)
        res = requests.get(ITEM_URL + '{}.json'.format(story_id))
        item = res.json()
        
        comments_list = []
        
        if 'kids' in item.keys():
            for comment in item['kids']:
                time.sleep(1)
                comment_res = requests.get(ITEM_URL + '{}.json'.format(comment))
                comment_item = comment_res.json()
                sub_comments_list = []

                if 'kids' in comment_item.keys():
                    for subcomment in comment_item['kids']:
                        time.sleep(1)
                        subcomment_res = requests.get(ITEM_URL + '{}.json'.format(subcomment))
                        subcomment_item = subcomment_res.json()
                        sub_comments_list.append(subcomment_item)
                try:
                    com_dt = datetime.datetime.fromtimestamp(comment_item['time'])
                    comments_list.append(
                            {
                                'id': comment_item['id'],
                                'by': comment_item.get('by',''),
                                'kids': sub_comments_list,
                                'parent': comment_item['parent'],
                                'time': str(com_dt),
                                'text': comment_item.get('text',''),
                                'type': comment_item.get('type','')

                            }
                        )
                except Exception as e:
                    print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
                    print(e)
                    continue
        try:            
            dt = datetime.datetime.fromtimestamp(item['time'])
            final_list.append({
                    'id':item['id'],
                    'by':item.get('by',''),
                    'kids':comments_list,
                    'time':str(dt),
                    'title':item['title'],
                    'text':item.get('text',''),
                    'type':item['type'],
                    'descendants': item['descendants'],
                    'score': item['score'],
                    'url': item.get('url','')
                })
        except Exception as e:
            print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
            print(e)
            continue

    return final_list


def insert_into_db(**context):
    context = context['task_instance']
    data = context.xcom_pull(task_ids='api_task')
    print("data")
    print(data)
    # data = api_task()
    storyQuery = 'INSERT INTO api_story (id,descendants,score,time,title,type,url,text,by) VALUES '
    commentQuery = 'INSERT INTO api_comment (id,by,text,time,type,story_id) VALUES '
    subCommentQuery = 'INSERT INTO api_subcomment (id,by,text,time,type,comment_id) VALUES '
    for i in data:
        storyQuery += '''({},{},{},'{}','{}','{}','{}','{}','{}'),'''.format(
            i['id'],
            i['descendants'],
            i['score'],
            i['time'],
            i['title'].replace("'","`"),
            i['type'],
            i['url'],
            i['text'].replace("'","`"),
            i['by'].replace("'","`")
            )
        
        for j in i['kids']:
            commentQuery += '''({},'{}','{}','{}','{}',{}),'''.format(
                j['id'],
                j['by'].replace("'","`"),
                j['text'].replace("'","`"),
                j['time'],
                j['type'],
                j['parent']
            )

            for k in j['kids']:
                comdt = datetime.fromtimestamp(k['time'])
                subCommentQuery += '''({},'{}','{}','{}','{}',{}),'''.format(
                    k['id'],
                    k.get('by','').replace("'","`"),
                    k.get('text','').replace("'","`"),
                    str(comdt),
                    k.get('type',''),
                    k['parent'])
    storyQuery = storyQuery[:-1]
    storyQuery += ';'

    commentQuery = commentQuery[:-1]
    commentQuery += ';'

    subCommentQuery = subCommentQuery[:-1]
    subCommentQuery += ';'

    print(storyQuery)
    print(commentQuery)
    print(subCommentQuery)

    execute_query(storyQuery)
    if len(commentQuery) > 70:
        print("Comments Integestion")
        execute_query(commentQuery)
    if len(subCommentQuery) > 70:
        print("Sub Comments Integestion")
        execute_query(subCommentQuery)



default_args = {
    'owner': 'Ahmad_hacker',
    # 'retries': 1,
    'start_date': days_ago(1),
    # 'retry_delay': timedelta(minutes=2)
}
dag_python = DAG(
    dag_id = 'hacker_news_ingestion',
    default_args = default_args,
    schedule_interval = '0 1 * * *',
    description = 'Getting fake name from different API',
    start_date=datetime.datetime(2023,1,5,1),
)
# dummy_task = DummyOperator(task_id='dummy_task', retries=3, dag=dag_python)
get_data_from_api = PythonOperator(task_id='api_task', python_callable=api_task, dag=dag_python)
ingestion_in_db = PythonOperator(task_id='ingestion_db', python_callable=insert_into_db,params={'result': get_data_from_api}, dag=dag_python)

get_data_from_api >> ingestion_in_db
if __name__ == "__main__":
    dag_python.cli() 
