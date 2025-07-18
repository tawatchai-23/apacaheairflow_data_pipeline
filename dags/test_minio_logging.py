from airflow.models.dag import DAG
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def test_minio_logging():
    logging.info("Testing MinIO S3 logging - INFO level")
    logging.warning("Testing MinIO S3 logging - WARNING level")
    logging.error("Testing MinIO S3 logging - ERROR level")
    
    # ทดสอบ log หลายบรรทัด
    for i in range(5):
        logging.info(f"Log message {i+1}")
    
    return "MinIO logging test completed"

dag = DAG(
    'test_minio_s3_logging',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'logging']
)

task = PythonOperator(
    task_id='test_minio_log_task',
    python_callable=test_minio_logging,
    dag=dag
)

task