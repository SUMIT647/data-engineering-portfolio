"""
Airflow DAG: Batch Data Pipeline Orchestration
Monthly ingestion and quarterly ETL processing
Author: Data Engineering Portfolio
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Monthly Ingestion DAG
with DAG(
    dag_id='monthly_data_ingestion',
    default_args=default_args,
    description='Monthly customer transaction data ingestion',
    schedule_interval='0 0 1 * *',  # Run at midnight on 1st of every month
    catchup=False,
    tags=['ingestion', 'monthly', 'batch'],
) as ingestion_dag:
    
    # Task 1: Validate source data
    validate_source = BashOperator(
        task_id='validate_source_data',
        bash_command='ls -lh /opt/airflow/data/customer_spending_1M_2018_2025.csv && echo "Source data validated"',
    )
    
    # Task 2: Run ingestion
    run_ingestion = BashOperator(
        task_id='run_data_ingestion',
        bash_command='cd /opt/airflow/ingestion && python ingest_data.py',
        env={
            'DATA_PATH': '/opt/airflow/data',
        }
    )
    
    # Task 3: Validate ingestion
    validate_ingestion = BashOperator(
        task_id='validate_ingestion',
        bash_command='ls -lh /opt/airflow/data/raw/ && echo "Ingestion validated"',
    )
    
    # Task 4: Send notification (placeholder)
    notify_ingestion_complete = BashOperator(
        task_id='notify_ingestion_complete',
        bash_command='echo "Monthly ingestion completed at $(date)"',
    )
    
    # Define task dependencies
    validate_source >> run_ingestion >> validate_ingestion >> notify_ingestion_complete


# Quarterly ETL and Feature Engineering DAG
with DAG(
    dag_id='quarterly_etl_feature_engineering',
    default_args=default_args,
    description='Quarterly batch ETL and feature engineering',
    schedule_interval='0 0 1 */3 *',  # Run at midnight on 1st day of every 3rd month (quarterly)
    catchup=False,
    tags=['etl', 'quarterly', 'batch', 'features'],
) as etl_dag:
    
    # Task 1: Check data availability
    check_data = BashOperator(
        task_id='check_raw_data',
        bash_command='ls -lh /opt/airflow/data/raw/ && echo "Raw data check completed"',
    )
    
    # Task 2: Run Spark ETL
    spark_etl = BashOperator(
        task_id='spark_etl_processing',
        bash_command='''
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.driver.memory=2g \
            --conf spark.executor.memory=2g \
            --conf spark.sql.shuffle.partitions=8 \
            --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar \
            /opt/airflow/spark-apps/spark_etl.py
        ''',
        env={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_USER': 'dataeng',
            'POSTGRES_PASSWORD': 'dataeng123',
            'POSTGRES_DB': 'feature_store',
            'POSTGRES_PORT': '5432',
        }
    )
    
    # Task 3: Validate features in database
    validate_features = BashOperator(
        task_id='validate_features',
        bash_command='echo "Feature validation placeholder - checking PostgreSQL tables"',
    )
    
    # Task 4: Create feature snapshot
    create_snapshot = BashOperator(
        task_id='create_feature_snapshot',
        bash_command='''
        mkdir -p /opt/airflow/data/snapshots
        echo "Feature snapshot created at $(date)" > /opt/airflow/data/snapshots/snapshot_$(date +%Y%m%d).txt
        ''',
    )
    
    # Task 5: Notify completion
    notify_etl_complete = BashOperator(
        task_id='notify_etl_complete',
        bash_command='echo "Quarterly ETL completed at $(date). Features ready for ML model training."',
    )
    
    # Define task dependencies
    check_data >> spark_etl >> validate_features >> create_snapshot >> notify_etl_complete


# Manual trigger DAG for testing
with DAG(
    dag_id='manual_full_pipeline',
    default_args=default_args,
    description='Manual trigger for full pipeline testing',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['manual', 'testing', 'full-pipeline'],
) as manual_dag:
    
    # Task 1: Run ingestion
    manual_ingestion = BashOperator(
        task_id='manual_ingestion',
        bash_command='cd /opt/airflow/ingestion && python ingest_data.py',
        env={'DATA_PATH': '/opt/airflow/data'}
    )
    
    # Task 2: Wait for ingestion
    wait_for_ingestion = BashOperator(
        task_id='wait_for_ingestion',
        bash_command='sleep 10 && echo "Ingestion completed"',
    )
    
    # Task 3: Run Spark ETL
    manual_spark_etl = BashOperator(
        task_id='manual_spark_etl',
        bash_command='''
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.driver.memory=2g \
            --conf spark.executor.memory=2g \
            --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar \
            /opt/airflow/spark-apps/spark_etl.py
        ''',
        env={
            'POSTGRES_HOST': 'postgres',
            'POSTGRES_USER': 'dataeng',
            'POSTGRES_PASSWORD': 'dataeng123',
            'POSTGRES_DB': 'feature_store',
        }
    )
    
    # Task 4: Generate report
    generate_report = BashOperator(
        task_id='generate_report',
        bash_command='''
        echo "===== PIPELINE EXECUTION REPORT =====" > /opt/airflow/data/pipeline_report.txt
        echo "Execution Date: $(date)" >> /opt/airflow/data/pipeline_report.txt
        echo "Status: SUCCESS" >> /opt/airflow/data/pipeline_report.txt
        cat /opt/airflow/data/pipeline_report.txt
        ''',
    )
    
    # Define task dependencies
    manual_ingestion >> wait_for_ingestion >> manual_spark_etl >> generate_report


# Data quality check DAG
with DAG(
    dag_id='data_quality_checks',
    default_args=default_args,
    description='Data quality validation checks',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['quality', 'validation', 'monitoring'],
) as quality_dag:
    
    # Task 1: Check for null values
    check_nulls = BashOperator(
        task_id='check_null_values',
        bash_command='echo "Null value check placeholder"',
    )
    
    # Task 2: Check data freshness
    check_freshness = BashOperator(
        task_id='check_data_freshness',
        bash_command='find /opt/airflow/data/raw -type f -mtime -7 | wc -l',
    )
    
    # Task 3: Validate schema
    validate_schema = BashOperator(
        task_id='validate_schema',
        bash_command='echo "Schema validation placeholder"',
    )
    
    # Task 4: Generate quality report
    quality_report = BashOperator(
        task_id='generate_quality_report',
        bash_command='echo "Data quality report generated at $(date)"',
    )
    
    # Define task dependencies
    [check_nulls, check_freshness, validate_schema] >> quality_report
