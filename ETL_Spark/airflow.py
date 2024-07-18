from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'Minh Pham',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dim_time',
    default_args=default_args,
    description='ETL Dim_Time with PySpark',
    schedule_interval=timedelta(days=1),
    start_date=timedelta(minutes=1),
    catchup=False,
)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='E:/ETL_Spark/pyspark_etl_script.py',  # Đường dẫn tới script PySpark
    conn_id='spark_default',
    packages='org.postgresql:postgresql:42.6.2',
    dag=dag,
)

spark_submit_task
