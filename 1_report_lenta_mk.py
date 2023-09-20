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
    'start_date': datetime(2023, 8, 12)
    }

#Интервал запуска DAG (МСК)
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_lenta_mku():

    # Получение данных из базы feed_actions
    @task()
    def extract_df():
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
            'database':'simulator_20230720',
            'user':'student', 
            'password':'dpo_python_2020'
            }

        # запрос        
        query = '''SELECT 
                    toDate(time) as date,
                    uniqExact(user_id) as DAU,
                    countIf(time, action = 'view') as views,
                    countIf(time, action = 'like') as likes,
                    ROUND(likes / views * 100, 3) as ctr
                FROM simulator_20230720.feed_actions
                WHERE toDate(time) between yesterday() - 7 AND yesterday()
                GROUP BY date
                ORDER BY date
                '''
                
        data = pandahouse.read_clickhouse(query, connection=connection)
        return data

    @task()
    def feed_report(data):
        msg = '''
        📑 Отчет по ленте новостей за {date} 📑
    (относительно значений метрики 1 день назад и 7 дней назад)
    DAU: {users} ({to_users_day_ago:+.2%}, {to_users_week_ago:+.2%})
    Views: {views} ({to_views_day_ago:+.2%}, {to_views_week_ago:+.2%})
    Likes: {likes} ({to_likes_day_ago:+.2%}, {to_likes_week_ago:+.2%})
    CTR: {ctr} ({to_ctr_day_ago:+.2%}, {to_ctr_week_ago:+.2%})
    '''

        yesterday = pd.Timestamp('now') - pd.DateOffset(days=1)
        day_ago = yesterday - pd.DateOffset(days=1)
        week_ago = yesterday - pd.DateOffset(days=7)
        
        data['date'] = pd.to_datetime(data['date']).dt.date
        data = data.astype({'DAU': int, 'views': int, 'likes': int})

        report = msg.format(date=yesterday.date(),
        
                            users=data[data['date'] == yesterday.date()]['DAU'].iloc[0],
                            to_users_day_ago=(data[data['date'] == yesterday.date()]['DAU'].iloc[0] -
                                            data[data['date'] == day_ago.date()]['DAU'].iloc[0])/
                                            data[data['date'] == day_ago.date()]['DAU'].iloc[0],
                            to_users_week_ago=(data[data['date'] == yesterday.date()]['DAU'].iloc[0] -
                                            data[data['date'] == week_ago.date()]['DAU'].iloc[0])/
                                            data[data['date'] == week_ago.date()]['DAU'].iloc[0],
                            
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
                                            data[data['date'] == week_ago.date()]['ctr'].iloc[0])
    
        return report

    @task()
    def make_plot(data):
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))        
        fig.suptitle('Статистика по ленте новостей за предыдущие 7 дней')

        plot_dict = {(0, 0): {'y': 'DAU', 'title': 'Уникальные пользователи'},
             (0, 1): {'y': 'views', 'title': 'Views'},
             (1, 0): {'y': 'likes', 'title': 'Likes'},
             (1, 1): {'y': 'ctr', 'title': 'CTR'}
            }

        for i in range(2):
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
        plot_object.name = 'Feed_status.png'   # присваиваем название объекту
        plt.close()   # закрываем объект

        return plot_object

    @task()
    def send(report, plot_object, chat_id):
        # инициализируем бота
        my_token='токен удален'
        bot = telegram.Bot(token=my_token)
        #chat_id = chat or -927780322   # my_chat=1079188742

        bot.sendMessage(chat_id=chat_id, text=report)    # отправляем текстовое сообщение
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)    # отправляем график


    data = extract_df()
    report = feed_report(data)
    plot_object = make_plot(data)
    send(report, plot_object, chat_id=-927780322)

report_lenta_mku = report_lenta_mku()
