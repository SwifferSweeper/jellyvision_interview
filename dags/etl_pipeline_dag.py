"""
Apache Airflow DAG for Benefits Engagement ETL Pipeline

This DAG orchestrates the ETL pipeline that processes raw events data and user data,
cleaning and transforming the data, and outputting Parquet files with Snappy compression.

DAG Schedule: Daily at 2:00 AM
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import the ETL pipeline
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from etl_pipeline import ETLPipeline


# DAG Configuration
DAG_ID = "etl_pipeline"
SCHEDULE = "0 2 * * *"  # Daily at 2:00 AM
START_DATE = datetime(2024, 1, 1)
RETENTION_DAYS = 30

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

    end = EmptyOperator(task_id="end")

    # Task dependencies
    start >> run_etl >> validate_output >> end
