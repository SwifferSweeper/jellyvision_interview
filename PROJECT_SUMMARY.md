# ETL Pipeline Project - Complete Setup

## What's Been Configured

Your ETL pipeline is now fully containerized with Apache Airflow and MinIO (S3-compatible storage). Here's what you have:

### ✅ Core Components
- **Apache Airflow 2.7.3** - Workflow orchestration with Celery executor
- **PostgreSQL 15** - Airflow metadata database  
- **Redis 7** - Message broker for Celery workers
- **MinIO** - S3-compatible object storage (replaces AWS S3 locally)
- **Your ETL Pipeline** - Processes events and user data into Parquet files

### ✅ Files Created/Updated

```
jellyvision_interview/
├── docker-compose.yaml        # All services configured
├── Dockerfile                 # Custom Airflow image
├── requirements.txt           # Python dependencies
├── etl_pipeline.py            # Your ETL code (updated with S3Uploader)
├── dags/
│   └── etl_pipeline_dag.py    # Airflow DAG (updated for MinIO)
├── init-airflow.sh            # Linux/Mac initialization script
├── init-airflow.ps1           # Windows PowerShell initialization script
├── Makefile                   # Convenient make commands
├── .dockerignore              # Optimize Docker builds
├── QUICKSTART.md              # 5-minute getting started guide
├── SETUP.md                   # Comprehensive setup documentation
└── PROJECT_SUMMARY.md         # This file
```

### ✅ What Changed

1. **RustFS → MinIO**: Replaced non-existent "RustFS" with MinIO (actual S3-compatible storage)
2. **Bucket Auto-Creation**: Added `minio-init` service to automatically create the `etl-outputs` bucket
3. **Environment Variables**: Added `AWS_ENDPOINT_URL` to all Airflow services for MinIO connectivity
4. **DAG Updated**: Changed endpoint from `http://rustfs:9000` to `http://minio:9000`
5. **Helper Scripts**: Created initialization scripts for both Windows and Linux/Mac
6. **Documentation**: Created comprehensive guides

## Quick Start (5 Minutes)

### 1. Initialize (First Time)

**Windows:**
```powershell
.\init-airflow.ps1
```

**Linux/Mac:**
```bash
chmod +x init-airflow.sh
./init-airflow.sh
```

### 2. Start Services
```bash
docker-compose up -d
```

### 3. Access UIs
- **Airflow**: http://localhost:8080 (admin/admin)
- **MinIO**: http://localhost:9001 (admin/admin123)

### 4. Run Pipeline
In Airflow UI → Toggle `etl_pipeline` ON → Click Play button

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                    │
├─────────────┬───────────────┬──────────────┬────────────────┤
│ PostgreSQL  │     Redis     │    MinIO     │  Airflow       │
│   :5432     │     :6379     │  :9000/:9001 │  :8080         │
└─────────────┴───────────────┴──────────────┴────────────────┘
```

**Service Dependencies:**
1. PostgreSQL & Redis start first (databases)
2. MinIO starts and becomes healthy
3. MinIO-init creates the `etl-outputs` bucket
4. Airflow services start (depend on all above)

## Workflow

```
┌──────────────────┐
│  Trigger DAG     │ (Manual or Scheduled)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Run ETL Pipeline │ Extract → Clean → Transform
│  (etl_pipeline.py)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Validate Outputs │ Check Parquet files exist
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Upload to S3    │ Upload to MinIO bucket
│     (MinIO)      │
└──────────────────┘
```

## Data Flow

1. **Input**: `raw_events.json` + `users.csv`
2. **Processing**: Clean, deduplicate, join, filter (US only)
3. **Local Output**: `output/clean_events.parquet` + `output/daily_summary.parquet`
4. **Cloud Storage**: Files uploaded to MinIO bucket `etl-outputs/{date}/`

## Environment Variables

Key configuration in `docker-compose.yaml`:

| Variable | Value | Purpose |
|----------|-------|---------|
| `AWS_ACCESS_KEY_ID` | admin | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | admin123 | MinIO secret |
| `AWS_ENDPOINT_URL` | http://minio:9000 | MinIO API endpoint |
| `AIRFLOW__CORE__EXECUTOR` | CeleryExecutor | Use Celery for scaling |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | false | Don't load example DAGs |

## Useful Commands

### Using Makefile
```bash
make init      # Initialize Airflow (first time)
make up        # Start all services
make down      # Stop services
make logs      # View all logs
make status    # Check service status
make clean     # Remove everything
make test      # Run ETL locally
```

### Using Docker Compose Directly
```bash
docker-compose build                      # Build images
docker-compose up -d                      # Start in background
docker-compose down                       # Stop services
docker-compose logs -f [service]          # View logs
docker-compose ps                         # Check status
docker-compose restart [service]          # Restart service
docker-compose exec [service] bash        # Enter container
```

### Airflow CLI
```bash
# Trigger DAG
docker-compose exec airflow-scheduler airflow dags trigger etl_pipeline

# List DAGs
docker-compose exec airflow-scheduler airflow dags list

# View DAG runs
docker-compose exec airflow-scheduler airflow dags list-runs -d etl_pipeline

# Check for import errors
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

## Testing

### Test Locally (No Airflow)
```bash
python etl_pipeline.py
```

### Test in Airflow
1. Go to http://localhost:8080
2. Trigger the `etl_pipeline` DAG
3. Monitor task execution
4. Check logs for each task

### Test S3 Upload
After running the pipeline, check MinIO:
1. Go to http://localhost:9001
2. Login with admin/admin123
3. Navigate to `etl-outputs` bucket
4. Verify files are uploaded

## Troubleshooting

### Services Not Starting
```bash
docker-compose logs [service-name]
docker-compose down && docker-compose up -d
```

### DAG Not Appearing
```bash
docker-compose exec airflow-scheduler airflow dags list-import-errors
docker-compose restart airflow-scheduler
```

### Connection Issues to MinIO
- Ensure MinIO is healthy: `docker-compose ps minio`
- Check MinIO logs: `docker-compose logs minio`
- Verify bucket exists: Go to http://localhost:9001

### Port Conflicts
Edit `docker-compose.yaml` to change ports:
```yaml
ports:
  - "8081:8080"  # Change external port
```

## Monitoring

### Airflow UI
- **DAGs**: View all workflows
- **Graph View**: See task dependencies
- **Gantt Chart**: Task execution timeline
- **Task Logs**: Detailed execution logs
- **XCom**: View task outputs

### MinIO Console
- **Buckets**: View storage buckets
- **Objects**: Browse uploaded files
- **Monitoring**: View storage metrics

## Next Steps

### For Development
1. Modify `etl_pipeline.py` to add your custom logic
2. Edit `dags/etl_pipeline_dag.py` to add more tasks
3. Add scheduling (e.g., `schedule_interval='@daily'`)
4. Configure email notifications on failures

### For Production
1. Use managed PostgreSQL (AWS RDS, Cloud SQL)
2. Use actual AWS S3 instead of MinIO
3. Set proper `AIRFLOW__CORE__FERNET_KEY`
4. Implement secrets management
5. Configure monitoring & alerting
6. Set up CI/CD pipeline
7. Use Kubernetes or ECS executor

## Resources

- **Airflow Docs**: https://airflow.apache.org/docs/
- **MinIO Docs**: https://min.io/docs/minio/
- **PyArrow Docs**: https://arrow.apache.org/docs/python/
- **Project Docs**:
  - `QUICKSTART.md` - Get started in 5 minutes
  - `SETUP.md` - Detailed setup and configuration
  - `README.md` - Project overview

## Support Contacts

For issues:
1. Check logs: `docker-compose logs -f`
2. Review documentation: `SETUP.md`
3. Verify prerequisites: Docker running, ports available
4. Check GitHub issues for common problems

## Security Notes

⚠️ **This setup is for LOCAL DEVELOPMENT ONLY**

Default credentials:
- Airflow: admin/admin
- MinIO: admin/admin123

For production:
- Use strong passwords
- Enable SSL/TLS
- Use secrets management
- Configure RBAC properly
- Set up network security

---

**You're all set!** 🎉

Start with: `docker-compose up -d`

Then visit: http://localhost:8080
