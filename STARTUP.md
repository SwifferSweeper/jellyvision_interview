# Getting Started Guide

This guide will help you set up and run the Benefits Engagement ETL Pipeline locally using Docker Compose.

## Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux)
- **Docker Compose** (included with Docker Desktop)

Verify installation:
```bash
docker --version
docker-compose --version
```

## Quick Start

### 1. Clone and Navigate

```bash
cd jellyvision_interview
```

### 2. Start All Services

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL** (port 5432) - Airflow metadata database
- **Redis** (port 6379) - Celery message broker
- **MinIO** (ports 9000, 9001) - S3-compatible object storage
- **Airflow Webserver** (port 8080) - Airflow UI
- **Airflow Scheduler** - Schedules and monitors DAGs
- **Airflow Worker** - Executes tasks

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / admin123 |

### 4. Run the ETL Pipeline

1. Open Airflow UI at http://localhost:8080
2. Login with `admin` / `admin`
3. Find the `etl_pipeline` DAG
4. Click **"Trigger DAG"** button (top right)
5. Watch the pipeline run in real-time

## Viewing Results

### Check Local Output Files

```bash
# List output files
ls -la output/

# View clean events (requires Python with pandas)
python -c "import pandas as pd; print(pd.read_parquet('output/clean_events.parquet').head())"
```

### Check MinIO Bucket

1. Open MinIO Console at http://localhost:9001
2. Login with `admin` / `admin123`
3. Click on "etl-outputs" bucket
4. Browse files by date prefix (e.g., `2026-02-05/`)

### View Airflow Task Logs

1. In Airflow UI, click on the DAG
2. Click on a task instance (e.g., `run_etl_pipeline`)
3. Click **"Log"** button to see detailed logs

## Project Structure

```
jellyvision_interview/
├── dags/
│   └── etl_pipeline_dag.py     # Airflow DAG definition
├── etl_pipeline.py              # Core ETL logic
├── Dockerfile                   # Custom Airflow image
├── docker-compose.yaml          # Service orchestration
├── requirements.txt            # Python dependencies
├── raw_events.json             # Input: Event data
├── users.csv                   # Input: User data
├── output/                     # Output: Parquet files
│   ├── clean_events.parquet
│   └── daily_summary.parquet
└── STARTUP.md                 # This file
```

## Configuration

### Environment Variables

All credentials are configured in `docker-compose.yaml`:

| Variable | Value | Service |
|----------|-------|---------|
| `AIRFLOW__CORE__EXECUTOR` | CeleryExecutor | All Airflow |
| `POSTGRES_USER` | airflow | PostgreSQL |
| `POSTGRES_PASSWORD` | airflow | PostgreSQL |
| `MINIO_ROOT_USER` | admin | MinIO |
| `MINIO_ROOT_PASSWORD` | admin123 | MinIO |
| `AWS_ACCESS_KEY_ID` | admin | Airflow→MinIO |
| `AWS_SECRET_ACCESS_KEY` | admin123 | Airflow→MinIO |

### Changing S3 Bucket

To use a different MinIO bucket:

1. Update [`dags/etl_pipeline_dag.py`](dags/etl_pipeline_dag.py:30):
   ```python
   S3_BUCKET = "your-bucket-name"
   ```

2. The bucket will be created automatically by the `minio-init` service.

## Troubleshooting

### Containers Won't Start

```bash
# Check container status
docker-compose ps

# View logs
docker-compose logs

# Common fix: Remove old containers and volumes
docker-compose down -v
docker-compose up -d
```

### Airflow Webserver Won't Start

```bash
# Check if ports are in use
netstat -ano | findstr :8080

# Kill conflicting process or change port in docker-compose.yaml
```

### MinIO Connection Issues

```bash
# Check MinIO health
docker-compose exec minio curl http://localhost:9000/minio/health/live

# Verify bucket exists
docker-compose exec minio mc ls myminio/
```

### DAG Not Appearing in Airflow

```bash
# Refresh DAGs
docker-compose exec airflow-webserver airflow dags list

# Check for import errors
docker-compose exec airflow-webserver python -c "import dags.etl_pipeline_dag"
```

### Task Failures

1. Check task logs in Airflow UI
2. Verify input files exist:
   ```bash
   ls -la raw_events.json users.csv
   ```
3. Ensure output directory has write permissions:
   ```bash
   mkdir -p output && chmod 777 output
   ```

## Stopping Services

```bash
# Stop all services (keeps volumes)
docker-compose down

# Stop and remove volumes (deletes all data!)
docker-compose down -v
```

## Development

### Running Locally (Without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Run ETL pipeline
python etl_pipeline.py

# Run tests
pytest tests/
```

### Rebuilding Custom Airflow Image

```bash
docker-compose build airflow-webserver
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                    │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │
│  │  PostgreSQL │  │    Redis    │  │       MinIO         │   │
│  │  (metadata) │  │  (broker)   │  │  (S3-compatible)   │   │
│  └─────────────┘  └─────────────┘  │  :9000, :9001      │   │
│                                    └─────────────────────┘   │
│                                                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │               Airflow Services                        │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────────┐   │   │
│  │  │Webserver  │  │ Scheduler │  │    Worker     │   │   │
│  │  │  :8080    │  │           │  │               │   │   │
│  │  └───────────┘  └───────────┘  └───────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘

Local Files
┌──────────────────────────────────────────────┐
│ raw_events.json → ETL Pipeline → clean_     │
│ users.csv       →              events.parquet│
│                              daily_summary.  │
│                              parquet         │
│                              ↓               │
│                         MinIO Bucket         │
└──────────────────────────────────────────────┘
```
