#!/bin/bash

# Airflow entrypoint script
set -e

echo "Starting Airflow initialization..."

# Wait for PostgreSQL to be ready
echo "Waiting for airflow-postgres to be ready..."
until python -c "import psycopg2; psycopg2.connect('dbname=airflow user=airflow password=airflow host=airflow-postgres')" 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "PostgreSQL is up"

# Initialize database
echo "Running database migration..."
airflow db migrate

# Create admin user if it doesn't exist
echo "Creating admin user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || echo "Admin user already exists"

echo "Starting Airflow webserver and scheduler..."

# Start both webserver and scheduler
exec bash -c "airflow webserver --port 8080 & airflow scheduler"
