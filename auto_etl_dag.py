from datetime import datetime, timedelta

import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Параметры подключения к базам данных
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                    'database':'simulator_20230220',
                    'user':'student', 
                    'password':'dpo_python_2020'
                    }
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                     'database':'test',
                     'user':'student-rw', 
                     'password':'656e2b0c9c'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'b-dalaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 23) 
}

# Интервал запуска DAG
schedule_interval = '0 12 * * *'

# Название колонок финальной таблицы
metrics = [
    'views', 'likes',
    'messages_received', 'messages_sent',
    'users_received', 'users_sent'
]

# Запросы для выгрузки данных из базы данных
query_feed = """
SELECT 
    toDate(time) AS event_date, 
    user_id AS user,
    gender, age, os,
    countIf(action='view') AS views,
    countIf(action='like') AS likes
FROM {db}.feed_actions 
WHERE event_date = yesterday()
GROUP BY event_date, user, gender, age, os
"""

query_messenger = """
SELECT
    event_date, user, gender, age, os,
    messages_received, messages_sent,
    users_received, users_sent
FROM
(SELECT 
    toDate(time) AS event_date, 
    user_id AS user,
    gender, age, os,
    count() AS messages_sent,
    uniq(reciever_id) AS users_sent
FROM simulator_20230220.message_actions
WHERE event_date = yesterday()
GROUP BY event_date, user, gender, age, os) q1
FULL OUTER JOIN
(SELECT 
    toDate(time) AS event_date, 
    reciever_id AS user,
    gender, age, os,
    count() AS messages_received,
    uniq(user_id) AS users_received
FROM simulator_20230220.message_actions
WHERE event_date = yesterday()
GROUP BY event_date, user, gender, age, os) q2
USING user
"""

# Запросы для создания таблиц
query_test = """
CREATE TABLE IF NOT EXISTS test.etl_table_dalaev_2 (
    event_date Date,
    dimension String,
    dimension_value String,
    views UInt64,
    likes UInt64,
    messages_received UInt64,
    messages_sent UInt64,
    users_received UInt64,
    users_sent UInt64
)
ENGINE = MergeTree()
ORDER BY event_date
"""


# Функция для получения датафрейма из базы данных Clickhouse
def ch_get_df(query='SELECT 1', connection=connection):
    df = ph.read_clickhouse(query, connection=connection)
    return df


# Функция для загрузки датафрейма в базу данных Clickhouse
def ch_load_df(df, query='SELECT 1', connection=connection_test):
    ph.execute(query=query, connection=connection)
    ph.to_clickhouse(
        df, 'etl_table_dalaev_2',
        connection=connection, index=False
    )


# Функция для DAG'а
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_dalaev():

    # Функция для извлечения данных
    @task
    def extract(query):
        df = ch_get_df(query=query)
        return df

    # Функция для объединения двух таблиц в одну
    @task
    def merge(df_feed, df_messenger):
        df_full = df_feed.merge(
            df_messenger,
            how='outer',
            on=['event_date', 'user', 'gender', 'age', 'os']
        ).dropna()
        df_full[metrics] = df_full[metrics].astype(int)
        return df_full
    
    # Функция для преобразования данных
    @task
    def transform(df, metric):
        gpoup_by = ['event_date', metric]
        columns = gpoup_by + metrics
        df_transform = df[columns] \
            .groupby(gpoup_by) \
            .sum() \
            .reset_index()
        return df_transform

    # Функция для сохранения данных в таблицу
    @task
    def load(df_gender, df_age, df_os):
        df_gender.rename(columns={'gender': 'dimension_value'}, inplace = True)
        df_gender.insert(1, 'dimension', 'gender')

        df_age.rename(columns={'age': 'dimension_value'}, inplace = True)
        df_age.insert(1, 'dimension', 'age')

        df_os.rename(columns={'os': 'dimension_value'}, inplace = True)
        df_os.insert(1, 'dimension', 'os')
        
        df_final = pd.concat([df_gender, df_age, df_os])
        
        ch_load_df(df_final, query_test)

    # Последовательный вызов задачей DAG'a
    df_feed = extract(query_feed)
    df_messenger = extract(query_messenger)
    
    df_full = merge(df_feed, df_messenger)
    
    df_gender = transform(df_full, 'gender')
    df_age = transform(df_full, 'age')
    df_os = transform(df_full, 'os')
    
    load(df_gender, df_age, df_os)

# Вызываем dag
dag_dalaev = dag_dalaev()