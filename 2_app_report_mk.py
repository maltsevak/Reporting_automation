import pandas as pd
import pandahouse
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram

from airflow.decorators import dag, task
from datetime import datetime, timedelta

sns.set()

#Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner':'k_maltseva_13',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 15)
    }

#Интервал запуска DAG (МСК)
schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
            'database':'simulator_20230720',
            'user':'student', 
            'password':'dpo_python_2020'}

q_dau_all = '''
        SELECT date, 
            uniqExact(user_id) as all_users, 
            countIf(DISTINCT user_id, os='iOS') as users_ios, 
            countIf(DISTINCT user_id, os='Android') as users_android
        FROM (
            SELECT toDate(time) as date, user_id, os
            FROM simulator_20230720.feed_actions
            WHERE toDate(time) between yesterday()-7 and yesterday()
            GROUP BY date, user_id, os
            
            UNION ALL
            
            SELECT toDate(time) as date, user_id, os
            FROM simulator_20230720.message_actions
            WHERE toDate(time) between yesterday()-7 and yesterday()
            GROUP BY date, user_id, os
            )
        GROUP BY date
        ORDER BY date
        '''

q_new_users = '''
        SELECT date, 
            uniqExact(user_id) as new_users, 
            countIf(DISTINCT user_id, source='ads') as new_users_ads, 
            countIf(DISTINCT user_id, source='organic') as new_users_organic
        FROM (
            SELECT MIN(start_date) as date, user_id, source

            FROM (
                SELECT MIN(toDate(time)) as start_date, user_id, source
                FROM simulator_20230720.feed_actions
                GROUP BY user_id, source
                
                UNION ALL
                
                SELECT MIN(toDate(time)) as start_date, user_id, source
                FROM simulator_20230720.message_actions
                GROUP BY user_id, source
                )
            GROUP BY user_id, source
            )
        WHERE date between yesterday()-7 and yesterday()
        GROUP BY date
        ORDER BY date
        '''

q_users_status = '''
        SELECT day as date, uniqExact(user_id) as users_status,
            countIf(DISTINCT user_id, status='only_feed') as users_only_feed,
            countIf(DISTINCT user_id, status='feed+message') as users_feed_message,
            countIf(DISTINCT user_id, status='only_message') as users_only_message
        FROM (
            SELECT *
            FROM (
                SELECT user_id, day, count_action, count_message, IF(count_message > 0, 'feed+message', 'only_feed') AS status
                FROM (
                    SELECT user_id,
                        DATE(time) as day,
                        COUNT(action) AS count_action
                    FROM simulator_20230720.feed_actions
                    WHERE day between yesterday()-7 and yesterday()
                    GROUP BY user_id, DATE(time)) t1

                    LEFT JOIN 

                    (SELECT user_id,
                        DATE(time) AS day,
                        COUNT(reciever_id) AS count_message
                    FROM simulator_20230720.message_actions
                    WHERE day between yesterday()-7 and yesterday()
                    GROUP BY user_id , DATE(time)) t2
                    USING user_id, day)

            UNION ALL

                (SELECT *
                FROM (
                    SELECT user_id, day, count_action, count_message, IF(count_action > 0, 'feed+message', 'only_message') AS status
                    FROM (
                        SELECT user_id,
                            DATE(time) as day,
                            COUNT(action) AS count_action
                        FROM simulator_20230720.feed_actions
                        WHERE day between yesterday()-7 and yesterday()
                        GROUP BY user_id, DATE(time)) t1

                        RIGHT JOIN 

                        (SELECT user_id,
                            DATE(time) AS day,
                            COUNT(reciever_id) AS count_message
                        FROM simulator_20230720.message_actions
                        WHERE day between yesterday()-7 and yesterday()
                        GROUP BY user_id , DATE(time)) t2

                        USING user_id, day
                        )
                    WHERE status == 'only_message'
                    )
                )
        GROUP BY date
        ORDER BY date
        '''
q_lenta_msg = '''
    SELECT *
    FROM
        (SELECT 
            toDate(time) as date,
            uniqExact(user_id) as users,
            countIf(time, action = 'view') as views,
            countIf(time, action = 'like') as likes,
            ROUND(likes / views * 100, 3) as ctr
        FROM simulator_20230720.feed_actions
        WHERE toDate(time) between yesterday()-7 AND yesterday()
        GROUP BY date
        ORDER BY date) t1
        
        JOIN
        
        (SELECT 
            toDate(time) as date,
            uniqExact(user_id) as users_msg,
            count(time) as count_msg
        FROM simulator_20230720.message_actions
        WHERE toDate(time) between yesterday()-7 AND yesterday()
        GROUP BY date
        ORDER BY date) t2 USING(date)
    '''

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def app_report_mku():

    # Получение данных из базы данных
    @task()
    def extract_df(query, connection):
        df = pandahouse.read_clickhouse(query=query, connection=connection)
        return df


    # объединение данных
    @task()
    def merge_df(df_dau_os, df_new, df_status, df_lenta_msg):
        data = pd.merge(df_dau_os, df_new, on='date')
        data = pd.merge(data, df_status, on='date')
        data = pd.merge(data, df_lenta_msg, on='date')

        return data


    @task()
    def report_text(data):
        msg = '''📑 ОТЧЁТ ПО ПРИЛОЖЕНИЮ за {date} 📑
(относительно значений метрик 1 день назад и 1 неделю назад)

Уникальные пользователи в приложении: 
    {all_users} ({to_all_users_day_ago:+.2%}, {to_all_users_week_ago:+.2%})
Уникальные пользователи в приложении по платформе:
    iOS: {users_ios} ({to_users_ios_day_ago:+.2%}, {to_users_ios_week_ago:+.2%})
    Android: {users_android} ({to_users_android_day_ago:+.2%}, {to_users_android_week_ago:+.2%})

Новые пользователи в приложении: 
    {new_users} ({to_new_users_day_ago:+.2%}, {to_new_users_week_ago:+.2%})
Новые пользователи по источнику:
    ads (рекламный): {new_users_ads} ({to_new_users_ads_day_ago:+.2%}, {to_new_users_ads_week_ago:+.2%})
    organic (органический): {new_users_organic} ({to_new_users_organic_day_ago:+.2%}, {to_new_users_organic_week_ago:+.2%})

Уникальные пользователи, использующие:
    - только ленту новостей: {users_only_feed} ({to_users_only_feed_day_ago:+.2%}, {to_users_only_feed_week_ago:+.2%})
    - ленту новостей и мессенджер: {users_feed_message} ({to_users_feed_message_day_ago:+.2%}, {to_users_feed_message_week_ago:+.2%})
    - только мессенджер: {users_only_message} ({to_users_only_message_day_ago:+.2%}, {to_users_only_message_week_ago:+.2%})

    ЛЕНТА:
DAU: {users} ({to_users_day_ago:+.2%}, {to_users_week_ago:+.2%})
Просмотры: {views} ({to_views_day_ago:+.2%}, {to_views_week_ago:+.2%})
Лайки: {likes} ({to_likes_day_ago:+.2%}, {to_likes_week_ago:+.2%})
CTR: {ctr} ({to_ctr_day_ago:+.2%}, {to_ctr_week_ago:+.2%})

    МЕССЕНДЖЕР:
DAU: {users_msg} ({to_users_msg_day_ago:+.2%}, {to_users_msg_week_ago:+.2%})
Количество сообщений: {count_msg} ({to_count_msg_day_ago:+.2%}, {to_count_msg_week_ago:+.2%})
'''

        yesterday = pd.Timestamp('now') - pd.DateOffset(days=1)
        day_ago = yesterday - pd.DateOffset(days=1)
        week_ago = yesterday - pd.DateOffset(days=7)

        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.astype({'all_users': int, 'users_ios': int, 'users_android': int,
                            'new_users': int,'new_users_ads': int,'new_users_organic': int,
                            'users_only_feed': int,'users_feed_message': int, 'users_only_message': int,
                            'users': int,'views': int, 'likes': int, 'ctr': int,'users_msg': int, 'count_msg': int})

        report = msg.format(date=yesterday.date(),
                    
                    all_users=data[data['date'] == yesterday.date()]['all_users'].iloc[0],
                    to_all_users_day_ago=(data[data['date'] == yesterday.date()]['all_users'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['all_users'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['all_users'].iloc[0],
                    to_all_users_week_ago=(data[data['date'] == yesterday.date()]['all_users'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['all_users'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['all_users'].iloc[0],
                    
                    users_ios=data[data['date'] == yesterday.date()]['users_ios'].iloc[0],
                    to_users_ios_day_ago=(data[data['date'] == yesterday.date()]['users_ios'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['users_ios'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['users_ios'].iloc[0],
                    to_users_ios_week_ago=(data[data['date'] == yesterday.date()]['users_ios'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['users_ios'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['users_ios'].iloc[0],
                    
                    users_android=data[data['date'] == yesterday.date()]['users_android'].iloc[0],
                    to_users_android_day_ago=(data[data['date'] == yesterday.date()]['users_android'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['users_android'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['users_android'].iloc[0],
                    to_users_android_week_ago=(data[data['date'] == yesterday.date()]['users_android'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['users_android'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['users_android'].iloc[0],
                    
                    
                    new_users=data[data['date'] == yesterday.date()]['new_users'].iloc[0],
                    to_new_users_day_ago=(data[data['date'] == yesterday.date()]['new_users'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['new_users'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['new_users'].iloc[0],
                    to_new_users_week_ago=(data[data['date'] == yesterday.date()]['new_users'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['new_users'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['new_users'].iloc[0],
                    
                    new_users_ads=data[data['date'] == yesterday.date()]['new_users_ads'].iloc[0],
                    to_new_users_ads_day_ago=(data[data['date'] == yesterday.date()]['new_users_ads'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['new_users_ads'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['new_users_ads'].iloc[0],
                    to_new_users_ads_week_ago=(data[data['date'] == yesterday.date()]['new_users_ads'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['new_users_ads'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['new_users_ads'].iloc[0],
                    
                    new_users_organic=data[data['date'] == yesterday.date()]['new_users_organic'].iloc[0],
                    to_new_users_organic_day_ago=(data[data['date'] == yesterday.date()]['new_users_organic'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['new_users_organic'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['new_users_organic'].iloc[0],
                    to_new_users_organic_week_ago=(data[data['date'] == yesterday.date()]['new_users_organic'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['new_users_organic'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['new_users_organic'].iloc[0],
                    
                    
                    users_only_feed=data[data['date'] == yesterday.date()]['users_only_feed'].iloc[0],
                    to_users_only_feed_day_ago=(data[data['date'] == yesterday.date()]['users_only_feed'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['users_only_feed'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['users_only_feed'].iloc[0],
                    to_users_only_feed_week_ago=(data[data['date'] == yesterday.date()]['users_only_feed'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['users_only_feed'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['users_only_feed'].iloc[0],
                    
                    users_feed_message=data[data['date'] == yesterday.date()]['users_feed_message'].iloc[0],
                    to_users_feed_message_day_ago=(data[data['date'] == yesterday.date()]['users_feed_message'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['users_feed_message'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['users_feed_message'].iloc[0],
                    to_users_feed_message_week_ago=(data[data['date'] == yesterday.date()]['users_feed_message'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['users_feed_message'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['users_feed_message'].iloc[0],
                    
                    users_only_message=data[data['date'] == yesterday.date()]['users_only_message'].iloc[0],
                    to_users_only_message_day_ago=(data[data['date'] == yesterday.date()]['users_only_message'].iloc[0] 
                                      - data[data['date'] == day_ago.date()]['users_only_message'].iloc[0])
                                      / data[data['date'] == day_ago.date()]['users_only_message'].iloc[0],
                    to_users_only_message_week_ago=(data[data['date'] == yesterday.date()]['users_only_message'].iloc[0] 
                                       - data[data['date'] == week_ago.date()]['users_only_message'].iloc[0])
                                       / data[data['date'] == week_ago.date()]['users_only_message'].iloc[0],

                    
                    users=data[data['date'] == yesterday.date()]['users'].iloc[0],
                    to_users_day_ago=(data[data['date'] == yesterday.date()]['users'].iloc[0] -
                                    data[data['date'] == day_ago.date()]['users'].iloc[0])/
                                    data[data['date'] == day_ago.date()]['users'].iloc[0],
                    to_users_week_ago=(data[data['date'] == yesterday.date()]['users'].iloc[0] -
                                    data[data['date'] == week_ago.date()]['users'].iloc[0])/
                                    data[data['date'] == week_ago.date()]['users'].iloc[0],
                            
                    views=data[data['date'] == yesterday.date()]['views'].iloc[0],
                    to_views_day_ago=(data[data['date'] == yesterday.date()]['views'].iloc[0] -
                                    data[data['date'] == day_ago.date()]['views'].iloc[0])/
                                    data[data['date'] == day_ago.date()]['views'].iloc[0],
                    to_views_week_ago=(data[data['date'] == yesterday.date()]['views'].iloc[0] -
                                    data[data['date'] == week_ago.date()]['views'].iloc[0])/
                                    data[data['date'] == week_ago.date()]['views'].iloc[0],
                            
                    likes=data[data['date'] == yesterday.date()]['likes'].iloc[0],
                    to_likes_day_ago=(data[data['date'] == yesterday.date()]['likes'].iloc[0] -
                                    data[data['date'] == day_ago.date()]['likes'].iloc[0])/
                                    data[data['date'] == day_ago.date()]['likes'].iloc[0],
                    to_likes_week_ago=(data[data['date'] == yesterday.date()]['likes'].iloc[0] -
                                    data[data['date'] == week_ago.date()]['likes'].iloc[0])/
                                    data[data['date'] == week_ago.date()]['likes'].iloc[0],

                            
                    ctr=data[data['date'] == yesterday.date()]['ctr'].iloc[0],
                    to_ctr_day_ago=(data[data['date'] == yesterday.date()]['ctr'].iloc[0] -
                                    data[data['date'] == day_ago.date()]['ctr'].iloc[0])/
                                    data[data['date'] == day_ago.date()]['ctr'].iloc[0],
                    to_ctr_week_ago=(data[data['date'] == yesterday.date()]['ctr'].iloc[0] -
                                    data[data['date'] == week_ago.date()]['ctr'].iloc[0])/
                                    data[data['date'] == week_ago.date()]['ctr'].iloc[0],
                                    

                    users_msg=data[data['date'] == yesterday.date()]['users_msg'].iloc[0],
                    to_users_msg_day_ago=(data[data['date'] == yesterday.date()]['users_msg'].iloc[0] -
                                    data[data['date'] == day_ago.date()]['users_msg'].iloc[0])/
                                    data[data['date'] == day_ago.date()]['users_msg'].iloc[0],
                    to_users_msg_week_ago=(data[data['date'] == yesterday.date()]['users_msg'].iloc[0] -
                                    data[data['date'] == week_ago.date()]['users_msg'].iloc[0])/
                                    data[data['date'] == week_ago.date()]['users_msg'].iloc[0],
                            
                    count_msg=data[data['date'] == yesterday.date()]['count_msg'].iloc[0],
                    to_count_msg_day_ago=(data[data['date'] == yesterday.date()]['count_msg'].iloc[0] -
                                    data[data['date'] == day_ago.date()]['count_msg'].iloc[0])/
                                    data[data['date'] == day_ago.date()]['count_msg'].iloc[0],
                    to_count_msg_week_ago=(data[data['date'] == yesterday.date()]['count_msg'].iloc[0] -
                                    data[data['date'] == week_ago.date()]['count_msg'].iloc[0])/
                                    data[data['date'] == week_ago.date()]['count_msg'].iloc[0]
                                    )
        
        return report


    @task()
    def make_plot_app(data):
        fig, axes = plt.subplots(3, figsize=(20, 12))  
        fig.suptitle('Статистика по всему приложению за 7 дней')

        plot_dict = {0: {'y': ['all_users', 'users_ios', 'users_android'], 'title': 'Уникальные пользователи'},
                    1: {'y': ['new_users','new_users_ads','new_users_organic'], 'title': 'Новые пользователи'},
                    2: {'y': ['users_only_feed','users_feed_message', 'users_only_message'], 
                    'title': 'Пользователи по характеру использования приложения'}}

        for i in range(3):
            for y in plot_dict[i]['y']:
                sns.lineplot(ax=axes[i], data=data, x='date', y=y)
            axes[i].set_title(plot_dict[(i)]['title'])
            axes[i].set(xlabel=None)
            axes[i].set(ylabel=None)
            axes[i].legend(plot_dict[i]['y'])
            for ind, label in enumerate(axes[i].get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
        
        plot_object = io.BytesIO()   # создаем объект
        plt.savefig(plot_object)   # сохраняем график в объект
        plot_object.seek(0)   # передвигаем курсор в начало
        plot_object.name = 'app_mku.png'   # присваиваем название объекту
        plt.close()   # закрываем объект

        return plot_object
    
    @task()
    def make_plot_lm(data):
        fig, axes = plt.subplots(3, 2, figsize=(20, 12))        
        fig.suptitle('Статистика по ЛЕНТЕ новостей и МЕССЕНДЖЕРУ за 7 дней')

        plot_dict = {(0, 0): {'y': 'users', 'title': 'Уникальные пользователи ЛЕНТЫ'},
             (0, 1): {'y': 'views', 'title': 'Просмотры'},
             (1, 0): {'y': 'likes', 'title': 'Лайки'},
             (1, 1): {'y': 'ctr', 'title': 'CTR'},
             (2, 0): {'y': 'users_msg', 'title': 'Уникальные пользователи МЕССЕНДЖЕРА'},
             (2, 1): {'y': 'count_msg', 'title': 'Количество сообщений'}
            }

        for i in range(3):
            for j in range(2):
                sns.lineplot(ax=axes[i, j], data=data, x='date', y=plot_dict[(i, j)]['y'])
                axes[i, j].set_title(plot_dict[(i, j)]['title'])
                axes[i, j].set(xlabel=None)
                axes[i, j].set(ylabel=None)
                for ind, label in enumerate(axes[i, j].get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                        
        plot_object = io.BytesIO()   # создаем объект
        plt.savefig(plot_object)   # сохраняем график в объект
        plot_object.seek(0)   # передвигаем курсор в начало
        plot_object.name = 'lm_status.png'   # присваиваем название объекту
        plt.close()   # закрываем объект

        return plot_object


    @task()
    def send(report, plot_object_app, plot_object_lm, chat=None):
        # инициализируем бота
        my_token='токен удален'
        bot = telegram.Bot(token=my_token)
        chat_id = chat or -927780322   
        # my_chat=1079188742

        bot.sendMessage(chat_id=chat_id, text=report)    # отправляем текстовое сообщение
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_app)    # отправляем график 1
        bot.sendPhoto(chat_id=chat_id, photo=plot_object_lm)    # отправляем график 2

    
    df_dau_os = extract_df(q_dau_all, connection)
    df_new = extract_df(q_new_users, connection)
    df_status = extract_df(q_users_status, connection)
    df_lenta_msg = extract_df(q_lenta_msg, connection)
    data = merge_df(df_dau_os, df_new, df_status, df_lenta_msg)
    report = report_text(data)
    plot_object_app = make_plot_app(data)
    plot_object_lm = make_plot_lm(data)
    send(report, plot_object_app, plot_object_lm, chat=None)


app_report_mku = app_report_mku()
