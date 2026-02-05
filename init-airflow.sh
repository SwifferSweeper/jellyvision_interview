#!/bin/bash
# Initialize Airflow - Correct Procedure
# This script properly initializes the Airflow database and creates an admin user

set -e

echo "=========================================="
echo "  Airflow Initialization Script"
echo "=========================================="
echo ""

# Step 1: Start databases
echo "Step 1: Starting PostgreSQL and Redis..."
docker-compose up -d postgres redis

echo "Waiting for databases to be healthy (15 seconds)..."
sleep 15

# Step 2: Initialize Airflow database
echo ""
echo "Step 2: Initializing Airflow database..."
docker-compose run --rm airflow-webserver airflow db init

# Step 3: Create admin user
echo ""
echo "Step 3: Creating admin user..."
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Step 4: Start all services
echo ""
echo "Step 4: Starting all services..."
docker-compose up -d

echo ""
echo "Waiting for services to start (30 seconds)..."
sleep 30

echo ""
echo "=========================================="
echo "  ✅ Airflow Initialization Complete!"
echo "=========================================="
echo ""
echo "Checking service status..."
docker-compose ps
echo ""
echo "Verifying DAG loaded..."
docker-compose exec airflow-scheduler airflow dags list
echo ""
echo "Access points:"
echo "  - Airflow UI: http://localhost:8080"
echo "  - MinIO Console: http://localhost:9001"
echo ""
echo "Credentials:"
echo "  Airflow  - Username: admin, Password: admin"
echo "  MinIO    - Username: admin, Password: admin123"
echo ""
echo "Next: Open http://localhost:8080 and trigger the etl_pipeline DAG!"
echo ""
