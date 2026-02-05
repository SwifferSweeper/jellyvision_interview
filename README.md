# Benefits Engagement ETL Pipeline

A production-ready ETL pipeline that processes mock benefits engagement data, outputting clean results as Parquet files with optional S3 storage. Can run standalone or orchestrated by Apache Airflow.

## Quick Start

### Option 1: Run with Docker Compose (Recommended)

See [STARTUP.md](STARTUP.md) for complete setup instructions:

```bash
docker-compose up -d
# Access Airflow UI: http://localhost:8080 (admin/admin)
# Access MinIO: http://localhost:9001 (admin/admin123)
```

### Option 2: Run Locally

```bash
pip install -r requirements.txt
python etl_pipeline.py
```

## Features

| Feature | Description |
|---------|-------------|
| **Data Cleaning** | Removes invalid rows, deduplicates, filters to US users |
| **Parquet Output** | Snappy-compressed Parquet files using pyarrow |
| **Airflow Orchestration** | Production DAG with logging, retries, validation |
| **S3 Storage** | Optional upload to S3-compatible storage (MinIO, AWS S3) |

## Architecture

```
raw_events.json → ETLPipeline → clean_events.parquet
users.csv       →             → daily_summary.parquet
                                    ↓
                              Optional S3 Upload
```

### Components

| Class | Responsibility |
|-------|-----------------|
| [`DataLoader`](etl_pipeline.py:31) | Extraction from JSON and CSV |
| [`DataCleaner`](etl_pipeline.py:96) | Cleaning operations |
| [`DataTransformer`](etl_pipeline.py:181) | Join, filter, aggregate |
| [`DataWriter`](etl_pipeline.py:253) | Write to Parquet |
| [`S3Uploader`](etl_pipeline.py:306) | Upload to S3 |
| [`ETLPipeline`](etl_pipeline.py:390) | Orchestrates ETL |

## Input/Output

### Input Files

| File | Description |
|------|-------------|
| `raw_events.json` | Event data with user_id, event_type, timestamp, value, metadata |
| `users.csv` | User data with user_id, signup_date, country |

### Output Files

| File | Description |
|------|-------------|
| `output/clean_events.parquet` | Cleaned, joined, filtered events (US users only) |
| `output/daily_summary.parquet` | Daily event counts per user |

## Data Cleaning Steps

1. Drop rows with missing `user_id`
2. Drop rows with invalid timestamps
3. Deduplicate events (user_id, event_type, timestamp, value)
4. Join with users table (add signup_date, country)
5. Filter to US users only

## Docker Services

| Service | Port | Description |
|---------|------|-------------|
| airflow-webserver | 8080 | Airflow UI |
| airflow-scheduler | - | DAG scheduler |
| airflow-worker | - | Task executor |
| minio | 9000, 9001 | S3-compatible storage |
| postgres | 5432 | Metadata database |
| redis | 6379 | Message broker |

## Configuration

### Airflow Variables (Optional)

Set in Airflow Admin → Variables:

| Key | Default | Description |
|-----|---------|-------------|
| `s3_bucket` | `etl-outputs` | S3 bucket name |
| `s3_endpoint_url` | `http://minio:9000` | S3 endpoint URL |
| `aws_access_key_id` | `admin` | AWS access key |
| `aws_secret_access_key` | `admin123` | AWS secret key |

## Running Tests

```bash
pytest tests/ -v
```

## Project Structure

```
├── dags/
│   └── etl_pipeline_dag.py     # Airflow DAG
├── tests/
│   ├── conftest.py
│   └── test_etl_pipeline.py
├── etl_pipeline.py              # Core ETL logic
├── Dockerfile                    # Custom Airflow image
├── docker-compose.yaml           # Service orchestration
├── requirements.txt            # Python dependencies
├── STARTUP.md                  # Complete setup guide
└── README.md                   # This file
```

## Requirements

### Core
- pandas>=2.0.0
- pyarrow>=12.0.0
- boto3>=1.26.0

### Development
- pytest>=7.0.0
- pytest-cov>=4.0.0

### Airflow (in Dockerfile)
- apache-airflow>=2.7.0

## License

MIT
