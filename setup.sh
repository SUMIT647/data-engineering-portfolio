#!/bin/bash

# Quick Start Script for Data Engineering Portfolio
# This script sets up and runs the entire data pipeline

set -e

echo "================================================================"
echo "Data Engineering Portfolio - Quick Start"
echo "================================================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

echo "✓ Docker is running"

# Check if dataset exists
if [ ! -f "data/customer_spending_1M_2018_2025.csv" ]; then
    echo "❌ Dataset not found!"
    echo "Please place 'customer_spending_1M_2018_2025.csv' in the 'data/' directory"
    exit 1
fi

echo "✓ Dataset found"

# Check Docker Compose version
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install Docker Compose."
    exit 1
fi

echo "✓ Docker Compose available"
echo ""

# Build images
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 1: Building Docker images..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker-compose build

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 2: Starting services..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker-compose up -d

echo ""
echo "Waiting for services to initialize (30 seconds)..."
sleep 30

# Check service health
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 3: Checking service health..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check PostgreSQL
if docker exec de-postgres psql -U dataeng -d feature_store -c "SELECT 1;" > /dev/null 2>&1; then
    echo "✓ PostgreSQL: Running"
else
    echo "✗ PostgreSQL: Not ready"
fi

# Check Spark
if curl -s http://localhost:8080 > /dev/null 2>&1; then
    echo "✓ Spark Master: Running"
else
    echo "✗ Spark Master: Not ready"
fi

# Check Airflow
if curl -s http://localhost:8081 > /dev/null 2>&1; then
    echo "✓ Airflow: Running"
else
    echo "✗ Airflow: Not ready (may need more time)"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 4: Running data ingestion..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker-compose run --rm ingestion

echo ""
echo "Waiting 10 seconds before ETL..."
sleep 10

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 5: Running Spark ETL..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker exec de-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar \
    /opt/spark-apps/spark_etl.py

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 6: Verifying results..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Query feature counts
docker exec de-postgres psql -U dataeng -d feature_store -c "
SELECT 'customer_features' as table_name, COUNT(*) as record_count FROM customer_features
UNION ALL
SELECT 'quarterly_features', COUNT(*) FROM quarterly_features
UNION ALL
SELECT 'payment_features', COUNT(*) FROM payment_features
UNION ALL
SELECT 'state_features', COUNT(*) FROM state_features
ORDER BY table_name;
"

echo ""
echo "================================================================"
echo "✓ SETUP COMPLETE!"
echo "================================================================"
echo ""
echo "Access your services:"
echo "  • Airflow UI:     http://localhost:8081 (admin/admin)"
echo "  • Spark Master:   http://localhost:8080"
echo "  • PostgreSQL:     localhost:5432 (dataeng/dataeng123)"
echo ""
echo "Useful commands:"
echo "  • View logs:      docker-compose logs -f"
echo "  • Stop services:  docker-compose down"
echo "  • Restart:        docker-compose restart"
echo "  • Run pipeline:   make pipeline"
echo ""
echo "================================================================"
