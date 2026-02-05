# Initialize Airflow - Correct Procedure (PowerShell)
# This script properly initializes the Airflow database and creates an admin user

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  Airflow Initialization Script" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Start databases
Write-Host "Step 1: Starting PostgreSQL and Redis..." -ForegroundColor Yellow
docker-compose up -d postgres redis

Write-Host "Waiting for databases to be healthy (15 seconds)..." -ForegroundColor Gray
Start-Sleep -Seconds 15

# Step 2: Initialize Airflow database
Write-Host ""
Write-Host "Step 2: Initializing Airflow database..." -ForegroundColor Yellow
docker-compose run --rm airflow-webserver airflow db init

# Step 3: Create admin user
Write-Host ""
Write-Host "Step 3: Creating admin user..." -ForegroundColor Yellow
docker-compose run --rm airflow-webserver airflow users create `
    --username admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com `
    --password admin

# Step 4: Start all services
Write-Host ""
Write-Host "Step 4: Starting all services..." -ForegroundColor Yellow
docker-compose up -d

Write-Host ""
Write-Host "Waiting for services to start (30 seconds)..." -ForegroundColor Gray
Start-Sleep -Seconds 30

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "  ✅ Airflow Initialization Complete!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""

Write-Host "Checking service status..." -ForegroundColor Cyan
docker-compose ps

Write-Host ""
Write-Host "Verifying DAG loaded..." -ForegroundColor Cyan
docker-compose exec airflow-scheduler airflow dags list

Write-Host ""
Write-Host "Access points:" -ForegroundColor Yellow
Write-Host "  - Airflow UI: http://localhost:8080"
Write-Host "  - MinIO Console: http://localhost:9001"
Write-Host ""
Write-Host "Credentials:" -ForegroundColor Yellow
Write-Host "  Airflow  - Username: admin, Password: admin"
Write-Host "  MinIO    - Username: admin, Password: admin123"
Write-Host ""
Write-Host "Next: Open http://localhost:8080 and trigger the etl_pipeline DAG!" -ForegroundColor Green
Write-Host ""
