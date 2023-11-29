from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.postgres_hook import PostgresHook
import json
import requests
import pandas as pd

# соединение с базой
conn_id = Variable.get("conn_name")

# Url 
URL_API= Variable.get("URL_API")

# Список акций
stocks = ['AAPL', 'NVDA', 'TSLA']

#Создаем таблицы row слоя
def create_tables():
        
    for stock in stocks:
        # соединяемся с БД
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()        
        table_name= f'{stock}'
        tbl_query= f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY, 
        row_date timestamp, 
        row_open double precision, 
        row_high double precision,
        row_low double precision,
        row_close double precision,
        row_volume int);
        """

        try:
                cursor.execute(tbl_query)
                conn.commit()

                cursor.close()
                conn.close()
                print(f"Таблица {stock} сoздана")

        except Exception as error:
             conn.rollback()
             raise Exception(f'Создать таблицу  {stock} не получилось: {error}!')

#Извлекаем данные из API
def extract_load_data():
    for stock in stocks:
        params = {
            "function":"TIME_SERIES_INTRADAY",
            "symbol":{stock},
            "interval":"1min",
            "outputsize":"full",
            "apikey": Variable.get("apikey")
            }
        table_name= f'{stock}'
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor() 
        try:          
             response = requests.get(URL_API, params)
             data = response.json()

             row_date = pd.DataFrame(data['Time Series (1min)'].keys())[0].tolist()
             row_open = pd.json_normalize(data['Time Series (1min)'].values())['1. open'].tolist() 
             row_high = pd.json_normalize(data['Time Series (1min)'].values())['2. high'].tolist() 
             row_low =  pd.json_normalize(data['Time Series (1min)'].values())['3. low'].tolist() 
             row_close = pd.json_normalize(data['Time Series (1min)'].values())['4. close'].tolist() 
             row_volume = pd.json_normalize(data['Time Series (1min)'].values())['5. volume'].tolist() 

             data_load = [(d,o,h,l,c,v) for d,o,h,l,c,v in zip(row_date, row_open, row_high, row_low, row_close,  row_volume)]

             cursor.executemany (f"INSERT INTO {table_name} (row_date, row_open, row_high, row_low, row_close, row_volume ) VALUES (%s,%s,%s,%s,%s,%s);", data_load)
             conn.commit()

             cursor.close()
             conn.close()
             print("Данные успешно загружены в таблицу!")
        except Exception as error:
             conn.rollback()
             raise Exception(f'Загрузить данные не получилось: {error}!')

#Создаем таблицы core слоя
def create_populate_core_tables():
        
    for stock in stocks:
        # соединяемся с БД
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()        
        core_table_name= f'core_{stock}'
        table_name= f'{stock}'
        company=Variable.get(f'{stock}')
        tbl_core_query= f"""
        DROP TABLE IF EXISTS {core_table_name};
        CREATE TABLE {core_table_name} (
        date timestamp,
        day_volume bigint,
        date_start timestamp, 
        day_open double precision,
        date_end timestamp, 
        day_close double precision,
        diff_exchange double precision,
        max_volume bigint,
        max_volume_interval timestamp,
        high double precision, 
        high_interval timestamp,
        low double precision,
        low_interval timestamp
        );
        """

        tbl_core_load_query=f"""
        INSERT INTO {core_table_name} 
        with {table_name}_cte as (
        select DATE_trunc('day', row_date) as date, 
        sum (row_volume) over (partition by DATE_trunc('day', row_date)) as day_volume,
        min(row_date) over (partition by DATE_trunc('day', row_date)) as date_start,
        first_value(row_open) over (partition by DATE_trunc('day', row_date)  order by row_date asc) as day_open,
        max(row_date) over (partition by DATE_trunc('day', row_date)) as date_end, 
        last_value(row_close) over (partition by DATE_trunc('day', row_date)) as day_close,
        (((first_value(row_open) over (partition by DATE_trunc('day', row_date) order by row_date asc)) -(last_value(row_close) over (partition by DATE_trunc('day', row_date) )))*100/(first_value(row_open) over (partition by DATE_trunc('day', row_date)  order by row_date asc))) as diff_exchange,
        max(row_volume)  over (partition by DATE_trunc('day', row_date)) as max_volume,
        max (row_high)  over (partition by DATE_trunc('day', row_date)) as high,
        min(row_low)  over (partition by DATE_trunc('day', row_date)) as low
        from {table_name} 
        group by DATE_trunc('day', row_date), row_volume,  row_open , row_close, row_high, row_low, row_date
        order by 1
        ),
        {table_name}_mv as (
        SELECT ac.date, a.row_date as max_volume_interval, ac.max_volume 
        FROM {table_name} a
        join {table_name}_cte  ac on a.row_volume = ac.max_volume
        group by date, row_date, ac.max_volume
        order by date
        ),
        {table_name}_mh as (
        SELECT ac.date, a.row_date as  high_interval, ac.high 
        FROM {table_name} a
        join {table_name}_cte  ac on a.row_high = ac.high and date = DATE_trunc('day', a.row_date)
        group by date, row_date, ac.high 
        order by date
        ),
       {table_name}_ml as (
       SELECT ac.date, a.row_date as  low_interval, ac.low 
       FROM {table_name} a
       join {table_name}_cte  ac on a.row_low = ac.low and date = DATE_trunc('day', a.row_date)
       group by date, row_date, ac.low 
       order by date
       )
       select ca.date, ca.day_volume, ca.date_start, ca.day_open, ca.date_end, ca.day_close, ca.diff_exchange, ca.max_volume, mv.max_volume_interval, ca.high, mh.high_interval, ca.low, ml.low_interval
       FROM {table_name}_cte ca
       join {table_name}_mv mv on ca.date = DATE_trunc('day', max_volume_interval)
       join {table_name}_mh  mh on ca.date = DATE_trunc('day', mh.high_interval)
       join {table_name}_ml ml on ca.date = DATE_trunc('day', ml.low_interval)
       group by ca.date, ca.day_volume, ca.date_start, ca.day_open, ca.date_end, ca.day_close, ca.diff_exchange, ca.max_volume, mv.max_volume_interval, ca.high, mh.high_interval, ca.low, ml.low_interval
       order by ca.date;
       ALTER TABLE {core_table_name} add ticker text DEFAULT '{stock}';
       ALTER TABLE {core_table_name} add company text DEFAULT '{company}';
       """

        try:
                cursor.execute(tbl_core_query)
                conn.commit()

                cursor.execute(tbl_core_load_query)
                conn.commit()

                cursor.close()
                conn.close()
                print(f"Таблица core_{stock} сoздана и заполнена")

        except Exception as error:
             conn.rollback()
             raise Exception(f'Создать и заполнить таблицу  {stock} не получилось: {error}!')
        
#Создаем витрину
def create_populate_mart():

     # соединяемся с БД
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()        
        mart_query= f"""
        DROP TABLE IF EXISTS mart;
        CREATE TABLE mart (
        ticker text,
        company text,
        day_volume bigint,
        day_open double precision,
        day_close double precision,
        diff_exchange double precision,
        max_volume_interval timestamp,
        high_interval timestamp,
        low_interval timestamp
        );
        """ 
        
        try:
                cursor.execute(mart_query)
                conn.commit()

                cursor.close()
                conn.close()
                print(f"Таблица mart сoздана")

        except Exception as error:
             conn.rollback()
             raise Exception(f'Создать таблицу  mart не получилось: {error}!') 
        
        for stock in stocks:
            # соединяемся с БД
            hook = PostgresHook(postgres_conn_id=conn_id)
            conn = hook.get_conn()
            cursor = conn.cursor() 
            core_table_name= f'core_{stock}'

            mart_load_query=f"""
            INSERT INTO mart 
            select ticker, company, day_volume, day_open, day_close, diff_exchange, max_volume_interval, high_interval, low_interval
            FROM {core_table_name}
            Order by date desc limit 1;
            """

            try:

                cursor.execute( mart_load_query)
                conn.commit()

                cursor.close()
                conn.close()
                print(f"Таблица mart заполнена данными {stock}")

            except Exception as error:
             conn.rollback()
             raise Exception(f'Заполнить таблицу  данными {stock} не получилось: {error}!')       


# аргументы дага по умолчанию
default_args = {
    "owner": "inna",
    "retries": 1,
    "retry_delay": 1,
    "start_date": datetime(2023, 11, 4),
}

with DAG(dag_id="get_rate", 
         default_args=default_args, 
         schedule_interval="@once", 
         description= "Получениe курса акций", 
         template_searchpath = "/tmp", 
         catchup=False) as dag:

    start = EmptyOperator(task_id='start') 
    end = EmptyOperator(task_id='end')

    create_stock_table = PythonOperator(
        task_id="create_row_stock_table",
         python_callable=create_tables
    )
    
    extract_load_data_row = PythonOperator(
        task_id='load_data_row',
        python_callable=extract_load_data
    )

    populate_core_table = PythonOperator(
        task_id='load_data_core',
        python_callable=create_populate_core_tables
    )

    create_mart = PythonOperator(
        task_id='mart',
        python_callable=create_populate_mart
    )

    start >> create_stock_table >> extract_load_data_row >> populate_core_table >> create_mart >>end

