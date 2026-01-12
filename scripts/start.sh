#!/bin/bash

echo "========================================"
echo "Smart City Data Platform - Setup"
echo "========================================"
echo ""

# Create .env file
echo "Creating .env file..."
cat > .env << EOF
AIRFLOW_UID=$(id -u)
AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW_PROJ_DIR=.
EOF

# Create directories
echo "Creating directories..."
mkdir -p dags logs plugins config spark-apps
chmod -R 777 logs dags plugins jobs data

# Initialize Airflow
echo ""
echo "Initializing Airflow database..."
docker-compose up airflow-init

echo ""
echo "========================================"
echo "Starting all services..."
echo "========================================"
docker-compose up -d

echo ""
echo "Waiting 60 seconds for services to start..."
sleep 60

echo ""
echo "========================================"
echo "Service Status:"
echo "========================================"
docker-compose ps

echo ""
echo "========================================"
echo "Access URLs:"
echo "========================================"
echo "Airflow:      http://localhost:8080"
echo "              Username: airflow"
echo "              Password: airflow"
echo ""
echo "Spark Master: http://localhost:8081"
echo "Spark Worker: http://localhost:8082"
echo "HDFS:         http://localhost:9870"
echo "Grafana:      http://localhost:3000 (admin/admin)"
echo "PgAdmin:      http://localhost:5050 (admin@admin.com/admin)"
echo ""
echo "========================================"
echo "Useful Commands:"
echo "========================================"
echo "View logs:    docker-compose logs -f [service-name]"
echo "Stop all:     docker-compose down"
echo "Restart:      docker-compose restart [service-name]"
echo "========================================"