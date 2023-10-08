from airflow import DAG
from airflow.models import Variable,Connection
from airflow.operators.python import PythonOperator

from datetime import datetime
import logging
from modules.kemen_scraper import KemenScraper
from modules.connector import Connector
from modules.transformer import Transformer

def fun_get_data_from_api(**kwargs):
    #get data
    scraper = KemenScraper(Variable.get('url_kemen_abc'))
    data = scraper.get_data()
    print(data.info())

    #create connector
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password= get_conn_postgres.password,
        db= get_conn_postgres.schema,
        port=get_conn_postgres.port
    )

    #drop table if exists
    try:
        p = "DROP table IF EXISTS kementrian_abc"
        engine_postgres.execute(p)
    except Exception as e:
        logging.error(e)
    
    #insert to mysql
    data.to_sql(con=engine_postgres, name= 'kementrian_abc', index=False)
    logging.info("DATA INSERTED SUCCESFULLY TO Postgres")

def fun_generate_dim(**kwargs):
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("MySQL")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password= get_conn_mysql.password,
        db= get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password= get_conn_postgres.password,
        db= get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    
    #insert to postgres
    transformer = Transformer(engine_sql,engine_postgres)
    transformer.create_dimension_case()
    transformer.create_dimension_province()
    transformer.create_dimension_district()

def fun_insert_province_daily(**kwargs):
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("MySQL")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password= get_conn_mysql.password,
        db= get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password= get_conn_postgres.password,
        db= get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
  
    #insert to postgres
    transformer = Transformer(engine_sql,engine_postgres)
    transformer.create_province_daily()

def fun_insert_district_daily(**kwargs):
    #create connector
    get_conn_mysql = Connection.get_connection_from_secrets("MySQL")
    get_conn_postgres = Connection.get_connection_from_secrets("Postgres")
    connector = Connector()
    engine_sql = connector.connect_mysql(
        host = get_conn_mysql.host,
        user = get_conn_mysql.login,
        password= get_conn_mysql.password,
        db= get_conn_mysql.schema,
        port=get_conn_mysql.port
    )
    engine_postgres = connector.connect_postgres(
        host = get_conn_postgres.host,
        user = get_conn_postgres.login,
        password= get_conn_postgres.password,
        db= get_conn_postgres.schema,
        port=get_conn_postgres.port
    )
    
    #insert to postgres
    transformer = Transformer(engine_sql,engine_postgres)
    transformer.create_district_daily()

with DAG(
    dag_id='BNSP_test',
    start_date=datetime(2022, 5, 28),
    schedule_interval='0 0 * * *',
    catchup=False
)as dag:

    op_get_data_from_api = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable=fun_get_data_from_api
    )
    op_generate_dim = PythonOperator(
        task_id = 'generate_dim',
        python_callable=fun_generate_dim
    )
    op_insert_province_daily = PythonOperator(
        task_id = 'insert_province_daily',
        python_callable=fun_insert_province_daily
    )
    op_insert_district_daily = PythonOperator(
        task_id = 'insert_district_daily',
        python_callable=fun_insert_district_daily
    )

op_get_data_from_api >> op_generate_dim
op_generate_dim >> op_insert_province_daily
op_generate_dim >> op_insert_district_daily