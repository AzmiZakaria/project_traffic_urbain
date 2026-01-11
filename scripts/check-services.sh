#!/bin/bash

echo "=========================================="
echo "Smart City Platform - Service Health Check"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_service() {
    local service=$1
    local url=$2
    local name=$3
    
    if docker-compose ps | grep -q "$service.*Up"; then
        if [ -n "$url" ]; then
            if curl -s -f "$url" > /dev/null 2>&1; then
                echo -e "${GREEN}✓${NC} $name is ${GREEN}UP${NC} and ${GREEN}HEALTHY${NC}"
            else
                echo -e "${YELLOW}⚠${NC} $name is ${YELLOW}UP${NC} but ${YELLOW}NOT RESPONDING${NC}"
            fi
        else
            echo -e "${GREEN}✓${NC} $name is ${GREEN}UP${NC}"
        fi
    else
        echo -e "${RED}✗${NC} $name is ${RED}DOWN${NC}"
    fi
}

echo "Core Services:"
echo "----------------------------------------"
check_service "postgres" "http://localhost:5432" "PostgreSQL"
check_service "zookeeper" "" "Zookeeper"
check_service "kafka" "" "Kafka"
echo ""

echo "Storage Services:"
echo "----------------------------------------"
check_service "namenode" "http://localhost:9870" "HDFS Namenode"
check_service "datanode" "" "HDFS Datanode"
echo ""

echo "Processing Services:"
echo "----------------------------------------"
check_service "spark-master" "http://localhost:9090" "Spark Master"
check_service "spark-worker" "" "Spark Worker"
echo ""

echo "Orchestration Services:"
echo "----------------------------------------"
check_service "airflow-webserver" "http://localhost:8081/health" "Airflow Webserver"
check_service "airflow-scheduler" "" "Airflow Scheduler"
echo ""

echo "Management Services:"
echo "----------------------------------------"
check_service "pgadmin" "http://localhost:5050" "PgAdmin"
check_service "grafana" "http://localhost:3000" "Grafana"
echo ""

echo "=========================================="
echo "Service URLs:"
echo "=========================================="
echo "Airflow:      http://localhost:8081 (airflow/airflow)"
echo "Spark Master: http://localhost:9090"
echo "HDFS:         http://localhost:9870"
echo "Grafana:      http://localhost:3000 (admin/admin)"
echo "PgAdmin:      http://localhost:5050 (admin@admin.com/admin)"
echo ""

echo "=========================================="
echo "Quick Commands:"
echo "=========================================="
echo "View all logs:       docker-compose logs -f"
echo "Restart all:         docker-compose restart"
echo "Stop all:            docker-compose down"
echo "Start all:           docker-compose up -d"
echo "Check status:        docker-compose ps"
echo "=========================================="