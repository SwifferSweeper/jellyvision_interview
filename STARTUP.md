# ETL Pipeline Setup Guide

This guide walks you through setting up and running the ETL pipeline with Apache Airflow and MinIO (S3-compatible storage).

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Setup                     │
├─────────────────┬───────────────┬──────────────┬────────────┤
│   PostgreSQL    │     Redis     │    MinIO     │  Airflow   │
│   (Metadata)    │   (Broker)    │  (S3 Store)  │  Services  │
└─────────────────┴───────────────┴──────────────┴────────────┘
```

## Initial Setup (Run Once)

### Step 1: Start Database Services
```bash
docker-compose up -d postgres redis
```

Wait 10 seconds for them to be healthy.

### Step 2: Initialize Airflow Database
```bash
docker-compose run --rm airflow-webserver airflow db init
```

This creates all the necessary tables in PostgreSQL.

### Step 3: Create Admin User
```bash
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### Step 4: Start All Services
```bash
docker-compose up -d
```

## Verification

Wait about 30 seconds, then check:

```bash
# Check all services are running
docker-compose ps

# Verify DAG is loaded
docker-compose exec airflow-scheduler airflow dags list

# Check webserver logs
docker-compose logs airflow-webserver
```

## Access Points

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (admin/admin123)

## Running the Pipeline

1. Go to http://localhost:8080
2. Login with admin/admin
3. Find `etl_pipeline` DAG
4. Toggle it ON (unpause)
5. Click the Play button to trigger

## Quick Commands

```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f airflow-scheduler

# Restart a service
docker-compose restart airflow-scheduler

# Stop everything
docker-compose down

# Full cleanup (removes volumes)
docker-compose down -v
```

## Troubleshooting

### "Relation 'log' does not exist"
This means the database wasn't initialized. Run:
```bash
docker-compose down
docker-compose up -d postgres redis
# Wait 10 seconds
docker-compose run --rm airflow-webserver airflow db init
docker-compose up -d
```

### Services Won't Start
```bash
docker-compose down
docker-compose up -d
docker-compose logs -f
```

### DAG Import Errors
```bash
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

## Automated Script (Windows PowerShell)

Save this as `start.ps1`:

```powershell
Write-Host "Starting databases..." -ForegroundColor Cyan
docker-compose up -d postgres redis

Write-Host "Waiting for databases to be healthy..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

Write-Host "Initializing Airflow..." -ForegroundColor Cyan
docker-compose run --rm airflow-webserver airflow db init

Write-Host "Starting all services..." -ForegroundColor Cyan
docker-compose up -d

Write-Host "`nWaiting for services to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host "`nChecking status..." -ForegroundColor Cyan
docker-compose ps

Write-Host "`n✅ Setup complete!" -ForegroundColor Green
Write-Host "`nAccess Airflow at: http://localhost:8080 (admin/admin)" -ForegroundColor Yellow
Write-Host "Access MinIO at: http://localhost:9001 (admin/admin123)" -ForegroundColor Yellow
```

Then run: `.\start.ps1`

## Current Status

✅ Database initialized  
✅ Admin user created  
✅ All services running  
✅ DAG loaded successfully  
✅ Ready to use!

Next: Open http://localhost:8080 and trigger your pipeline!
