#  телеграмм бот, который раз в день присылает отчёт в 11:00, сравнение графиков по разным метрикам и файл со сводкой

from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import StringIO
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'b-dalaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 26),
}

schedule_interval = '0 11 * * *'


def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

my_token = '5924546775:AAHIvowHeAHc-Rl8Lm5Gsyw4NusyriyGVMA'
bot = telegram.Bot(token=my_token)

chat_id=-802518328


@dag(default_args = default_args, schedule_interval = schedule_interval, catchup= False)
def dag_dalaev_7_2():

    @task()
    def get_df_data():
        q= """SELECT
t1.day, t1.month, t1.user_id, t1.country, t1.source,
t1.views, t1.likes, t1. CTR, t1.views_per_user, t1.likes_per_user,
t2.messages, t2.messages_per_user
FROM (SELECT toDate(toStartOfDay(time)) AS day,
toDate(toStartOfMonth(day)) AS month,
user_id,
country,source,countIf(post_id, action='view') AS views,
countIf(post_id, action='like') AS likes,round(likes / views, 2) as CTR,
round(countIf(action='view') / countDistinctIf(user_id, action='view'), 2) AS views_per_user,
round(countIf(action='like') / countDistinctIf(user_id, action='like'), 2) AS likes_per_user 
FROM simulator_20230220.feed_actions
WHERE toDate(time) between today()-15 and yesterday()
GROUP BY day, user_id, os, gender, country, source
ORDER BY day ) AS t1
LEFT JOIN
( SELECT toDate(toStartOfDay(time)) AS day, toDate(toStartOfMonth(day)) AS month,
user_id,
country,
source,
count(user_id) AS messages,
round(count(user_id) / countDistinct(user_id), 2) AS messages_per_user
FROM simulator_20230220.message_actions
WHERE toDate(time) between today()-15 and yesterday()
GROUP BY day, user_id, country, source
ORDER BY day) AS t2
ON t1.user_id = t2.user_id AND t1.day = t2.day
format TSVWithNames"""
        df = ch_get_df(query=q)
        return df
    
    @task()
    def send_message(df):
        df_yesterday = df[df['day'] == max(df['day'])]
        df_yesterday_group = df_yesterday.groupby('day', as_index=False).agg({'user_id': 'count', 'views': 'sum', 'likes' : 'sum', 'CTR': 'mean', 'views_per_user': 'mean', 'likes_per_user':'mean', 'messages':'sum', 'messages_per_user': 'mean'})
        dau=df_yesterday_group.iloc[0]['user_id']
        views=df_yesterday_group.iloc[0]['views']
        likes=df_yesterday_group.iloc[0]['likes']
        CTR=df_yesterday_group.iloc[0]['CTR']
        views_per_user=df_yesterday_group.iloc[0]['views_per_user']
        likes_per_user=df_yesterday_group.iloc[0]['likes_per_user']
        messages=df_yesterday_group.iloc[0]['messages']
        messages_per_user=df_yesterday_group.iloc[0]['messages_per_user']
        
        users_russia=round(df_yesterday[df_yesterday['country'] == 'Russia'].shape[0] / df_yesterday.shape[0] ,4)
        users_ads = round(df_yesterday[df_yesterday['source'] == 'ads'].shape[0] / df_yesterday.shape[0] ,4)
        users_organic = round(df_yesterday[df_yesterday['source'] == 'organic'].shape[0] / df_yesterday.shape[0] ,4)
        active_users= df_yesterday[df_yesterday['messages'] > 0].shape[0]
        read_only= df_yesterday[df_yesterday['messages'] == 0].shape[0]
        
        bot_text = '_'*15 + '\n' \
        + 'Ключевые показатели за вчера: \n\n' \
        + f'DAU: {dau:,} \n' \
        + f'Просмотры: {views:,} \n' \
        + f'Лайки: {likes:,} \n' \
        + f'CTR: {round(CTR*100,2)}% \n' \
        + f'Просмотры в среднем: {round(views_per_user,2)} \n' \
        + f'Лайки в среднем: {round(likes_per_user,2)} \n' \
        + f'Сообщений всего: {messages} \n' \
        + f'Сообщений в среднем: {round(messages_per_user,2)} \n' \
        + '-'*15 + '\n' \
        + f'Пользователей из России: {round(users_russia*100,2)}% \n' \
        + f'Органических пользователей: {round(users_organic*100,2)}% \n' \
        + f'Рекламных пользователей: {round(users_ads*100,2)}% \n' \
        + f'Активных пользователей: {round(active_users / dau*100,2)}% \n' \
        + f'Только читающих ленту: {round(read_only / dau*100,2)}% \n'
        
        bot.sendMessage(chat_id=chat_id, text=bot_text)

    
    @task()
    def make_final(df):
        df['date'] = pd.to_datetime(df['day'])
        curr_week=df['date'].max().isocalendar()
        last_week=df['date'].max() - pd.Timedelta(7, unit='day')
        last_week=last_week.to_pydatetime().isocalendar()
        last_week_df=df.loc[(df['date'].dt.isocalendar().week == last_week[1]) & (df['date'] <= (df['date'].max() - pd.Timedelta(7, unit='day')))]
        curr_week_df=df.loc[df['date'].dt.isocalendar().week == curr_week[1]]
        df_week_dynamic=pd.concat([last_week_df, curr_week_df])
        df_wd_group=df_week_dynamic.groupby(pd.Grouper(key='date', freq='W')).agg({'user_id': 'count', 'views': 'sum', 'likes' : 'sum', 'CTR': 'mean', 'views_per_user': 'mean', 'likes_per_user':'mean', 'messages':'sum', 'messages_per_user': 'mean'})
        final_data=pd.concat([df_wd_group, df_wd_group.diff()[1:]])
        final_data.set_index(pd.Index(['Previous_week', 'This_week', 'Difference']),'user_id', inplace=True)
        final_data.rename(columns={"user_id": "Active_users"}, inplace=True)
        return final_data
    
    @task()
    def send_csv(final_data, df):
        file_object = io.StringIO()
        final_data.to_csv(file_object)
        file_object.name = 'Динамика.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
    
    @task()
    def send_graph(final_data):
        diff_g=round(final_data.iloc[-1:]/final_data.iloc[0],4) *100
        long_df = pd.melt(diff_g)
        ax=sns.barplot(y = long_df.variable, x = long_df.value, estimator=sum, ci=False, orient='h')
        ax.set_title('Динамика основных показателей по сравнению с прошлой неделей, %')
        plot_object = io.BytesIO()
        plt.savefig(plot_object, bbox_inches='tight')
        plot_object.seek(0)
        plot_object.name = 'Динамика основных показателей по сравнению с прошлой неделей, %.jpg'
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    get_df  = get_df_data()
    bot_send_message = send_message(get_df)
    make_df = make_final(get_df)
    bot_send_csv = send_csv(make_df, get_df)
    bot_send_graph = send_graph(make_df)
    
dag_dalaev_7_2=dag_dalaev_7_2()
