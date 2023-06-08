from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from move_gcs_to_bigquery import move_gcs_to_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
}

dag = DAG(
    'gcs_spark_bigquery_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval= '0 7,13 * * *'    # It runs at 7a.m and 1p.m every day
)
# Create a Spark Cluster
#########################

# Step 1:Execute Spark job written in Python (OK)
spark_job = DataProcPySparkOperator(
    task_id='spark_job',
    main='gs://it-analytics-inventory-380100-dev/processing_resources/spark_processing.py',
    job_name='spark-job-processing',
    cluster_name='dev-group-booking-6515',
    region='us-central1',
    dag=dag
)

# Step 2: Move data from GCS to BigQuery (OK)
gcs_to_bigquery = PythonOperator(
    task_id='gcs_to_bigquery',
    python_callable=move_gcs_to_bigquery,
    op_kwargs={
        'bucket_name': 'it-analytics-inventory-380100-dev',
        'directory_path': 'datalake/functional/group_booking',
        # 'object_name': 'part-00000-a7904191-a032-443c-ab6d-9863ad1f2362-c000.snappy.parquet', ##?
        'table_name': 'group_booking',
        'project_id': 'round-bounty-380100',
        'dataset_id': 'booking'
    },
    dag=dag
)

# Delete Spark Cluster
##########################

# Set up task dependencies
spark_job >> gcs_to_bigquery

