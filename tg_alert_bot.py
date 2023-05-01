# телеграмм бот проверяет на аномалии каждые 15 минут

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import pandahouse as ph
import io
import seaborn as sns
import matplotlib.pyplot as plt
import telegram
from datetime import datetime, timedelta, date

bot_token = '5924546775:AAHIvowHeAHc-Rl8Lm5Gsyw4NusyriyGVMA'
bot = telegram.Bot(token=bot_token)
chat_id = -958942131


connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230220'
}

query_feednews = """
            select toStartOfFifteenMinutes(time) as period, 
                   formatDateTime(period, '%R') as hm,
                   countIf(action = 'view') as views,
                   countIf(action = 'like') as likes,
                   count(Distinct user_id) as active_users,
                   (likes / views) * 100 as CTR
            from simulator_20230220.feed_actions
            where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            group by period
            order by period
"""

query_messages = """
            select toStartOfFifteenMinutes(time) as period, 
                   formatDateTime(period, '%R') as hm,
                   count(Distinct user_id) as Active_users,
                   count(*) as Messages
            from simulator_20230220.message_actions
            where time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            group by period
            order by period
"""

default_args = {
    'owner': 'b-dalaev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 28),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

def check_anomaly(metric, df, n = 5, a = 3):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['upper_border'] = df['q75'] + df['iqr'] * a
    df['lower_border'] = df['q25'] - df['iqr'] * a

    current_metric = df[metric].iloc[-1]

    if current_metric > df['upper_border'].iloc[-1] or current_metric < df['lower_border'].iloc[-1]:
        return True, df
    else:
        return False, df

def get_plot(df, metric, product):
    rows = len(df) - 4 * 7
    fig, ax = plt.subplots(figsize = (18,8))
    ax.yaxis.grid()
    plt.xticks(rotation=315)
    ax.plot('hm', metric, data = df.iloc[rows:], color='green', lw=2, label = metric) 
    ax.plot('hm', 'upper_border', data = df.iloc[rows:], color='red', ls=':', lw=2.5, label = 'upper_border', ) 
    ax.plot('hm', 'lower_border', data = df.iloc[rows:], color='blue', linestyle='--', lw=2, label = 'lower_border') 
    ax.legend(loc='upper left')

    fig.suptitle(f"Current plot for {metric} in {product}", fontsize=18, fontweight='bold')
    fig.tight_layout(pad = 3)

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = f"Plot {metric} {product}.png"
    plt.close()

    return plot_object

def get_message(df, metric, product):
    current_value = df[metric].iloc[-1]
    upper_border = df['upper_border'].iloc[-1]
    lower_border = df['lower_border'].iloc[-1]

    if current_value > upper_border:
        deviation = round((current_value / upper_border - 1) * 100, 2)
    elif current_value < lower_border:
        deviation = round((current_value / lower_border - 1) * 100, 2)

    text = f"""
Metric {metric} in product {product}.
Current value: {current_value}. 
Deviation: {deviation}%.
    """
    return text

        
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def bot_dalaev_8_1():
    
    @task
    def extract_data(query):
        df_data = ph.read_clickhouse(query, connection=connection)
        return df_data

    @task
    def transform_realtime_data(df, product: str):
        metrics = df.columns[2:]
        df_common_data = df.iloc[:,:2]
        dict_anomaly = {}
        
        for metric in metrics:
            df_metric_data = df[metric]
            df_current_metric_data = pd.concat([df_common_data, df_metric_data], axis = 1)
            anomaly, df_current_metric_data = check_anomaly(metric, df_current_metric_data)
            
            if anomaly:
                plot = get_plot(df_current_metric_data, metric, product)
                message = get_message(df_current_metric_data, metric, product)
                pr_metric = f"{product}_{metric}"
                dict_anomaly[pr_metric] = [message, plot]
                
        return dict_anomaly
    
    @task
    def send_alert(chat_id, dict_anomaly):
        key_anomaly = dict_anomaly.keys()

        for key in key_anomaly:
            bot.send_message(chat_id=chat_id, text=dict_anomaly[key][0])
            bot.sendPhoto(chat_id=chat_id, photo=dict_anomaly[key][1])
    
    df_feednews = extract_data(query_feednews)
    df_messages = extract_data(query_messages)
    dict_anomaly_fn = transform_realtime_data(df_feednews, 'Feednews')
    dict_anomaly_m = transform_realtime_data(df_messages, 'Messages')
    send_alert(chat_id = chat_id, dict_anomaly = dict_anomaly_fn)
    send_alert(chat_id = chat_id, dict_anomaly = dict_anomaly_m)

bot_dalaev_8_1 = bot_dalaev_8_1()