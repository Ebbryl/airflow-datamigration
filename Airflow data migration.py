import pandas_gbq
import pydata_google_auth
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators import bigquery_operator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import psycopg2 as pg
import pandas as pd

default_args = {
    "owner": "tim1",
    #"depends_on_past": False,
    "start_date": datetime(2019, 8, 23),
    "email": ["ebbryl.rochman@tokopedia.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    #"retry_delay": timedelta(minutes=1),
}

dag = DAG("final_project", default_args=default_args)

#1
def bq_connection(ds, **ags):
    SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
    ]
    credentials = pydata_google_auth.get_user_credentials(
    SCOPES,
    auth_local_webserver=False)
    print('the connection is available')
    return credentials

#2
def bq_python(ds, **ags):
    #pull the data from previous task
    credentials = ags['task_instance'].xcom_pull(task_ids='bq_connection')
    #conditional: if not exist, select all, else: select just the newest (check to the bq)
    query = "select distinct order_id, user_id\
            FROM `acube_2019.acube_fintech_final_project_2019`\
                limit 10"
    constellation = pandas_gbq.read_gbq(
        query=query,
        project_id='minerva-da-coe',
        credentials=credentials
    )
    print(constellation.head())
    return constellation
#3 Take the model from other place[s]

#4 Perform prediction
def model(ds, **ags):
    df = ags['task_instance'].xcom_pull(task_ids='start_query')
    # df = pd.DataFrame({'name': ['yusita', 'dea', 'ebbryl'],
    #             'label': [0,0,1]})
    df.to_csv('/usr/local/airflow/dags/labeling.csv',index = False, sep = ',', header = False)

#5
def load_to_postgres(ds, **ags):
    conn = pg.connect("host=postgres dbname=airflow user=airflow password=airflow")
    cur = conn.cursor()
    f = open('/usr/local/airflow/dags/labeling.csv', 'r')
    f.seek(0)
    cur.copy_from(f, 'labeling', sep=',')
    #cur.copy_from(df, 'labels', sep=',')
    conn.commit()
    f.close()
    

t1 = PythonOperator(
    task_id='bq_connection',
    provide_context=True,
    python_callable=bq_connection,
    xcom_push=True,
    dag=dag)

t2 = PythonOperator(
    task_id='start_query',
    provide_context=True,
    python_callable=bq_python,
    dag=dag)

t3 = PythonOperator(
    task_id = 'modelling',
    provide_context = True,
    python_callable = model,
    dag=dag)

t4 = PostgresOperator(
    task_id='create_table',
    postgres_conn_id="postgres_default",
    sql="create table if not exists labeling(order_id varchar(50), user_id varchar(50))",
    database="airflow",
    dag=dag)
    
t5 = PostgresOperator(
    task_id='truncate_table',
    postgres_conn_id="postgres_default",
    sql="truncate table labeling;",
    database="airflow",
    dag=dag)

t6 = PythonOperator(
    task_id='load_to_postgres',
    provide_context=True,
    python_callable=load_to_postgres,
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
t5.set_upstream(t4)
t6.set_upstream(t3)
t6.set_upstream(t5)