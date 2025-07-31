import telegram
import numpy as np
import seaborn as sns
import matplotlib as plt
import io
import pandas as pd
import pandahouse as ph

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dylknklhnl',
    'user': 'student',
    'database': 'simulator'
}

bot_token='8012634757:AAGeOHbHVolniuUBmAx3XF1IcpBzX9Vfq_Ls'
bot=telegram.Bot(token=bot_token)

updates=bot.getUpdates()
print(updates[-1])

default_args = {
    'owner': 'v-nazemkina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 7, 20),
}

schedule_interval = '0 23 * * *'
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def dag_bot():
    @task()
    def extract_ctr():
        query_1=""" select countIf(action='like')/countIf(action='view') as ctr, toDate(time) as date
                    from simulator_20250620.feed_actions 
                    where toDate(time)=today()-1
                    group by date   
            """
        df_1=ph.read_clickhouse(query=query_1, connection=connection)
        return df_1
    @task()
    def extract_likes():
        query_2="""select countIf(action='like') likes, toDate(time) as date
                    from simulator_20250620.feed_actions 
                    where toDate(time)=today()-1
                    group by date   
            """
        df_2=ph.read_clickhouse(query=query_2, connection=connection)
        return df_2
    
    @task()
    def extract_views():
        query_3="""select countIf(action='view') views , toDate(time) as date
                    from simulator_20250620.feed_actions 
                    where toDate(time)=today()-1
                    group by date   
            """
        df_3=ph.read_clickhouse(query=query_3, connection=connection)
        return df_3
    @task()
    def extract_dau():
        query_4="""select count(distinct user_id), toDate(time) as date
                    from simulator_20250620.feed_actions 
                    where toDate(time)=today()-1
                    group by date   
            """
        df_4=ph.read_clickhouse(query=query_4, connection=connection)
        return df_4
    @task()
    def extract_dau_week():
        query_5="""select count(distinct user_id), toDate(time) as date
                    from simulator_20250620.feed_actions 
                    where toDate(time) between today()-7 and today()
                    group by date   
            """
        df_5=ph.read_clickhouse(query=query_5, connection=connection)
        return df_5
    @task()
    def extract_ctr_week():
        query_6="""select countIf(action='like')/countIf(action='view') as ctr, toDate(time) as date
                        from simulator_20250620.feed_actions 
                        where toDate(time) between today()-7 and today()
                        group by date   
                """
        df_6=ph.read_clickhouse(query=query_6, connection=connection)
        return df_6
    @task()
    def extract_likes_views_week():
        query_7="""select countIf(action='like') likes, countIf(action='view') views, toDate(time) as date
                        from simulator_20250620.feed_actions 
                        where toDate(time) between today()-7 and today()
                        group by date   
                """
        df_7=ph.read_clickhouse(query=query_7, connection=connection)
        return df_7
    
    @task()
    def message_bot(df_1, df_2, df_3,df_4):
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.strftime("%d-%m-%Y")
        report_text = f"""
        <b>Отчет за </b> {  yesterday}:
        <b>DAU:</b> {df_4}
        <b>Просмотры:</b> {df_3}
        <b>Лайки:</b> {df_2}
        <b>CTR:</b> {df_1:.2%}
        """
        bot.sendMessage(chat_id=1181229884, text=report_text, parse_mode='HTML')
    @task()
    def dau_week_img(df_5):
        plt.figure(figsize=(8, 6))
        sns.lineplot(data=df_5, x="date", y="uniqExact(user_id)", color='blue',linewidth=2)
        plt.xticks(rotation=20, fontsize=8)  
        plt.xticks(fontsize=10)
        plt.xlabel('Date', fontsize=12)  
        plt.ylabel('DAU', fontsize=12)    
        plt.title('DAU', fontsize=14, fontweight='bold') 
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300)  
        plot_object.seek(0)
        plt.close()
        bot.send_photo(chat_id=1181229884, photo=plot_object)
            
    @task()
    def ctr_week_img(df_6):
        plt.figure(figsize=(8, 6))
        sns.lineplot(data=df_5, x="date", y="ctr", color='blue',linewidth=2)
        plt.xticks(rotation=20, fontsize=8)  
        plt.xticks(fontsize=10)
        plt.xlabel('Date', fontsize=12)  
        plt.ylabel('CTR', fontsize=10)    
        plt.title('CTR', fontsize=14, fontweight='bold') 
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300)  
        plot_object.seek(0)
        plt.close()
        bot.send_photo(chat_id=1181229884, photo=plot_object)
            
    @task()
    def likes_views_week_img(df_7):
        plt.figure(figsize=(8, 6))
        sns.lineplot(data=df_7)
        plt.xticks(rotation=20, fontsize=8)  
        plt.xticks(fontsize=10)
        plt.xlabel('Date', fontsize=12)    
        plt.title('Лайки и просмотры', fontsize=14, fontweight='bold') 
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png', dpi=300)  
        plot_object.seek(0)
        plt.close()
        bot.send_photo(chat_id=1181229884, photo=plot_object)
            
        df_1=extract_ctr()
        df_3=extract_views()
        df_2=extract_likes()
        df_4=extract_dau()
        df_5=extract_dau_week()
        df_6=extract_ctr_week()
        df_7=extract_likes_views_week()
        
        message_bot(df_1, df_2, df_3, df_4)
        image_bot_dau(df_5)
        image_bot_views(df_7)
        image_bot_likes(df_7)
        image_bot_ctr(df_6)
dag_bot=dag_bot()
        
            
            

      


        
        
