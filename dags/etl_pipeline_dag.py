"""
Apache Airflow DAG for Benefits Engagement ETL Pipeline

This DAG orchestrates the ETL pipeline that processes raw events data and user data,
cleaning and transforming the data, and outputting Parquet files with Snappy compression.

DAG Schedule: None (manual trigger only)
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import the ETL pipeline
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from etl_pipeline import ETLPipeline, S3Uploader


# DAG Configuration
DAG_ID = "etl_pipeline"
SCHEDULE = None  # Manual trigger only
START_DATE = datetime(2024, 1, 1)
RETENTION_DAYS = 30

# S3 Configuration
S3_BUCKET = "{{ var.value.get('s3_bucket', 'etl-outputs') }}"
S3_PREFIX = "{{ ds }}"
S3_ENDPOINT_URL = "{{ var.value.get('s3_endpoint_url', None) }}"  # For MinIO/S3-compatible storage

# Default arguments for all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["data-team@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def get_base_paths():
    """Get base paths for the ETL pipeline."""
    # Assuming the DAG is in dags/ and data is in the project root
    base_path = Path(__file__).parent.parent
    return {
        "events_path": base_path / "raw_events.json",
        "users_path": base_path / "users.csv",
        "output_dir": base_path / "output",
    }


def run_etl_pipeline(**context):
    """Execute the ETL pipeline and return statistics."""
    paths = get_base_paths()
    
    pipeline = ETLPipeline(
        events_path=paths["events_path"],
        users_path=paths["users_path"],
        output_dir=paths["output_dir"],
    )
    
    stats = pipeline.run()
    
    # Push stats to XCom for reporting
    context["ti"].xcom_push(key="etl_stats", value=stats)
    
    return stats


def validate_outputs(**context):
    """Validate that ETL outputs were created successfully."""
    paths = get_base_paths()
    clean_events_path = paths["output_dir"] / "clean_events.parquet"
    daily_summary_path = paths["output_dir"] / "daily_summary.parquet"
    
    errors = []
    
    if not clean_events_path.exists():
        errors.append(f"Missing output file: {clean_events_path}")
    
    if not daily_summary_path.exists():
        errors.append(f"Missing output file: {daily_summary_path}")
    
    if errors:
        raise FileNotFoundError("\n".join(errors))
    
    # Get stats from previous task
    stats = context["ti"].xcom_pull(key="etl_stats")
    
    if stats:
        print(f"ETL Pipeline completed successfully:")
        print(f"  - Clean events: {stats.get('clean_events_count', 'N/A')} rows")
        print(f"  - Daily summary: {stats.get('daily_summary_count', 'N/A')} rows")
        print(f"  - Total dropped: {stats.get('dropped_cleaning', 0) + stats.get('dropped_dedup', 0) + stats.get('dropped_us_filter', 0)} rows")


def upload_to_s3(**context):
    """Upload Parquet files to S3."""
    from airflow.models import Variable
    
    paths = get_base_paths()
    
    # Get S3 configuration from Airflow Variables or use defaults
    s3_bucket = Variable.get("s3_bucket", default_var=S3_BUCKET)
    s3_prefix = context.get("ds", S3_PREFIX)  # Use execution date as prefix
    s3_endpoint = Variable.get("s3_endpoint_url", default_var=S3_ENDPOINT_URL)
    
    # Get AWS credentials from Airflow Connections or Variables
    aws_access_key = Variable.get("aws_access_key_id", default_var=None)
    aws_secret_key = Variable.get("aws_secret_access_key", default_var=None)
    
    uploader = S3Uploader(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        endpoint_url=s3_endpoint,
    )
    
    # Upload Parquet files
    results = uploader.upload_parquet_files(paths["output_dir"])
    
    print("Files uploaded to S3:")
    for filename, s3_uri in results.items():
        print(f"  - {filename}: {s3_uri}")
    
    # Push S3 URIs to XCom
    context["ti"].xcom_push(key="s3_uploads", value=results)
    
    return results


# Define the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Benefits Engagement ETL Pipeline - Extract, Transform, Load",
    schedule_interval=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "pipeline", "benefits"],
) as dag:

    # Task definitions
    start = EmptyOperator(task_id="start")

    run_etl = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl_pipeline,
        provide_context=True,
        doc="""Execute the Benefits Engagement ETL Pipeline
        
        This task:
        - Extracts events from raw_events.json and users from users.csv
        - Cleans data (removes invalid rows, deduplicates)
        - Joins events with user data
        - Filters to US users only
        - Creates daily event summaries
        - Writes outputs to output/ directory as Parquet files
        """,
    )

    validate_output = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_outputs,
        provide_context=True,
        doc="Validate that all ETL output files were created successfully",
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
        doc="""Upload Parquet files to S3-compatible storage
        
        This task uploads clean_events.parquet and daily_summary.parquet
        to the configured S3 bucket using the execution date as prefix.
        """,
    )

    end = EmptyOperator(task_id="end")

    # Task dependencies
    start >> run_etl >> validate_output >> upload_to_s3_task >> end
