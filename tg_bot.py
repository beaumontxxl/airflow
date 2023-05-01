# телеграмм бот, который раз в день присылает отчёт в 11:00, по указанным метрикам за вчера и за прошлую неделю

import telegram
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import io
from io import StringIO
import pandas as pd
from airflow.decorators import dag, task
from datetime import timedelta
from datetime import datetime


# параметры бота
my_token = '5924546775:AAHIvowHeAHc-Rl8Lm5Gsyw4NusyriyGVMA'
bot = telegram.Bot(token=my_token)
chat_id = -802518328


# функция для подключения к кликхаусу
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# параметры дага
default_args = {
    'owner': 'b-dalaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 25),
}

date_yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_weekago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

@dag(default_args=default_args, schedule_interval='0 11 * * *', catchup=False)

# сам даг
def dag_dalaev_7_1():
    @task
    def get_feed_yesterday():
        query = """SELECT user_id, toDate(time) event_date, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) = today()-1
                GROUP BY event_date, user_id
                format TSVWithNames"""
        feed_yesterday = ch_get_df(query=query)
        return feed_yesterday
 
    @task
    def get_dau(feed_yesterday):
        dau = feed_yesterday.user_id.nunique()
        return dau

    @task
    def get_likes(feed_yesterday):
        likes = feed_yesterday.likes.sum()
        return likes

    @task
    def get_views(feed_yesterday):
        views = feed_yesterday.views.sum()
        return views

    @task
    def get_ctr(likes, views):
        ctr = (likes / views).round(3)
        return ctr

    @task
    def get_feed_week():
        query_week = """SELECT user_id, toDate(time) date, 
                countIf(action = 'like') likes,
                countIf(action = 'view') views
                FROM simulator_20230220.feed_actions
                WHERE toDate(time) < today() and toDate(time) >= today()-7
                GROUP BY date, user_id
                order by date asc
                format TSVWithNames"""
        feed_week = ch_get_df(query=query_week)
        return feed_week

    @task
    def send_summary(date_weekago, date_yesterday, dau, likes, views, ctr, feed_week):
        print('trying to send message')
        bot.send_message(chat_id, f'''Метрики за {date_yesterday}:
DAU: {dau} , CTR: {ctr}
Likes: {likes}, Views: {views}

Графики метрик за {date_weekago} - {date_yesterday} :''')
        print('message was sent')

        media = []

        def append_media(data, y_axis, title):
            plt.figure()
            sns.lineplot(x='date', y=y_axis, data=data)
            plt.xticks(rotation=20)
            plt.title(title)
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = title + '.png'
            plt.close()
            media.append(telegram.InputMediaPhoto(plot_object))

        print('trying to fill media group')
        append_media(feed_week.groupby('date').views.sum().reset_index(), 'views', 'Views')
        append_media(feed_week.groupby('date').likes.sum().reset_index(), 'likes', 'Likes')
        append_media(feed_week.groupby('date').user_id.nunique().reset_index().rename(columns={'user_id': 'users_nunique'}),
            'users_nunique', 'DAU')

        likes_views_week = feed_week[['date', 'likes', 'views']].groupby('date').sum().reset_index()
        likes_views_week['ctr'] = (likes_views_week['likes'] / likes_views_week['views']).round(3)
        append_media(likes_views_week, 'ctr', 'CTR')
        print('trying to send media group')
        bot.send_media_group(chat_id, media)
        print('media group was sent')

    feed_yesterday = get_feed_yesterday()
    dau = get_dau(feed_yesterday)
    likes = get_likes(feed_yesterday)
    views = get_views(feed_yesterday)
    ctr = get_ctr(likes, views)
    feed_week = get_feed_week()
    send_summary(date_weekago, date_yesterday, dau, likes, views, ctr, feed_week)

dag_dalaev_7_1 = dag_dalaev_7_1()