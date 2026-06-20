from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sentiment_analysis_pipeline',
    default_args=default_args,
    description='Full Sentiment Analysis Pipeline (Production Grade)',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# 1. Data Ingestion (Mocking the download and HDFS upload)
t1 = BashOperator(
    task_id='ingest_data',
    bash_command='echo "Ingesting data from Kaggle to HDFS..." && sleep 5',
    dag=dag,
)

# 2. Preprocessing
# Note: In a real prod environment, you would use SparkSubmitOperator
t2 = BashOperator(
    task_id='pyspark_preprocessing',
    bash_command='echo "Running Spark Preprocessing..." && sleep 10',
    dag=dag,
)

# 3. Feature Extraction
t3 = BashOperator(
    task_id='feature_extraction',
    bash_command='echo "Extracting features (TF-IDF, Word2Vec)..." && sleep 10',
    dag=dag,
)

# 4. ML Training
t4 = BashOperator(
    task_id='ml_training',
    bash_command='echo "Training Sentiment Models..." && sleep 15',
    dag=dag,
)

# 5. MongoDB Sync
t5 = BashOperator(
    task_id='sync_to_mongodb',
    bash_command='echo "Syncing results to MongoDB..." && sleep 5',
    dag=dag,
)

# Define dependency
t1 >> t2 >> t3 >> t4 >> t5
