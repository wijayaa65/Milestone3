'''
=================================================
Milestone 3

Nama  : Andryan kalmer wijaya
Batch : RMT030

program ini dibuat untuk melakukan data cleaning dan mengupload ke elasticsearch
=================================================
'''



from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def clean_data():
    # Connection parameters
    database = "airflow"
    username = "airflow"
    password = "airflow"
    host = "postgres"

    # Create PostgreSQL connection URL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Create SQLAlchemy engine and connect
    engine = create_engine(postgres_url)
    conn = engine.connect()

    # Load data from PostgreSQL
    df = pd.read_sql_query("SELECT * FROM datam3", conn)

    # Define the necessary columns
    columns = [
        'customer_id', 'gender', 'age', 'under_30', 'senior_citizen', 'married', 'dependents',
        'number_of_dependents', 'country', 'state', 'city', 'zip_code', 'latitude', 'longitude',
        'population', 'quarter', 'referred_a_friend', 'number_of_referrals', 'tenure_in_months',
        'offer', 'phone_service', 'avg_monthly_long_distance_charges', 'multiple_lines',
        'internet_service', 'internet_type', 'avg_monthly_gb_download', 'online_security',
        'online_backup', 'device_protection_plan', 'premium_tech_support', 'streaming_tv',
        'streaming_movies', 'streaming_music', 'unlimited_data', 'contract', 'paperless_billing',
        'payment_method', 'monthly_charge', 'total_charges', 'total_refunds',
        'total_extra_data_charges', 'total_long_distance_charges', 'total_revenue',
        'satisfaction_score', 'customer_status', 'churn_label', 'churn_score', 'cltv'        
    ]
    # Keep only necessary columns
    df = df[columns]
    # Remove duplicate rows
    df = df.drop_duplicates()
    # Remove rows with null values
    df = df.dropna()
    # Save cleaned data to CSV
    df.to_csv('/opt/airflow/data/P2M3_Andryan_wijaya_data_clean.csv', sep=',', index=False)


def upload_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/data/P2M3_Andryan_wijaya_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="datam3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")


default_args = {
    'owner': 'Andre',
    'start_date': datetime(2024, 5, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('Run_dag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5),
         ) as dag:

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

clean_data_task >> upload_data
