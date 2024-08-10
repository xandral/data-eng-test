from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from scripts.decode_data import decode_data
from scripts.load_to_postgres import load_to_postgres
from scripts.update_operations import update_data

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'extract_and_load_dag',
    default_args=default_args,
    description='A simple extract and load DAG',
    schedule_interval='@daily',
    params={
        'commoncrawl_url': 'http://example.com/segment.gz'  # Default URL, can be overridden
    },
)

# Task to download data using Python function
download_task = PythonOperator(
    task_id='decode_data',
    python_callable=decode_data,
    dag=dag,
)

# Task to download Common Crawl segment using BashOperator
wget_task = BashOperator(
    task_id='download_commoncrawl_segment',
    bash_command="""wget -O /opt/airflow/scripts/raw_data {{ params.commoncrawl_url }}""",
    dag=dag,
)

# Task to load data into PostgreSQL
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# Task to update data
update_task = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    dag=dag,
)


download_task >> wget_task >> load_task >> update_task
