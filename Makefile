.PHONY: help init up down restart logs build clean test

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

init: ## Initialize Airflow (run once before first start)
	@echo "Building Docker images..."
	docker-compose build
	@echo "Initializing Airflow database..."
	docker-compose run --rm airflow-webserver airflow db init
	@echo "Creating admin user..."
	docker-compose run --rm airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin || true
	@echo ""
	@echo "✓ Initialization complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Run 'make up' to start all services"
	@echo "  2. Access Airflow UI at http://localhost:8080 (admin/admin)"
	@echo "  3. Access MinIO Console at http://localhost:9001 (admin/admin123)"

up: ## Start all services
	docker-compose up -d
	@echo ""
	@echo "✓ Services started!"
	@echo ""
	@echo "Access points:"
	@echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "  - MinIO Console: http://localhost:9001 (admin/admin123)"
	@echo ""
	@echo "Run 'make logs' to view logs"

down: ## Stop all services
	docker-compose down

restart: ## Restart all services
	docker-compose restart

logs: ## View logs from all services
	docker-compose logs -f

logs-airflow: ## View Airflow logs only
	docker-compose logs -f airflow-webserver airflow-scheduler airflow-worker

logs-minio: ## View MinIO logs
	docker-compose logs -f minio

build: ## Rebuild Docker images
	docker-compose build --no-cache

clean: ## Stop and remove all containers, volumes, and images
	docker-compose down -v --rmi local
	rm -rf output/*.parquet

test: ## Run the ETL pipeline locally (without Airflow)
	python etl_pipeline.py

status: ## Show status of all services
	docker-compose ps
