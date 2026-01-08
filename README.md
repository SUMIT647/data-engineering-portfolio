PROJECT OVERVIEW:
A complete data engineering pipeline for customer transaction analysis featuring 
data ingestion, ETL processing, feature engineering, and workflow orchestration 
using Docker, Spark, PostgreSQL, and Airflow.

================================================================================
                    MANUAL STEP-BY-STEP STARTUP


Step 1: Navigate to the project directory in PowerShell
Step 2: Copy 'customer_spending_1M_2018_2025.csv' to the data/ folder
Step 3: Build Docker images by running: docker-compose build
Step 4: Start all services in the background by running: docker-compose up -d
Step 5: Wait 30 seconds for services to initialize
Step 6: Verify PostgreSQL is running: docker exec de-postgres psql -U dataeng -d feature_store -c "SELECT 1;"
Step 7: Verify Spark Master is running: curl http://localhost:8080
Step 8: Verify Airflow is running: curl http://localhost:8081
Step 9: Run data ingestion: docker-compose run --rm ingestion
Step 10: Wait 10 seconds after ingestion completes
Step 11: Run Spark ETL: docker exec de-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.driver.memory=2g --conf spark.executor.memory=2g --jars /opt/bitnami/spark/jars/postgresql-42.6.0.jar /opt/spark-processing/spark_etl.py
Step 12: Pipeline execution is complete and features are ready

================================================================================
                        SERVICE ACCESS DETAILS

PostgreSQL Database:
  Hostname: localhost
  Port: 5432
  Username: dataeng
  Password: dataeng123
  Database: feature_store
  Tables: raw_transactions, customer_features, quarterly_features, payment_features, state_features, pipeline_execution_log

Apache Spark Master UI:
  URL: http://localhost:8080
  Used for monitoring Spark cluster status, jobs, and resource allocation

Apache Airflow Web UI:
  URL: http://localhost:8081
  Username: airflow
  Password: airflow
  Used for scheduling, monitoring, and manually triggering DAGs

================================================================================
                         DATA PIPELINE FLOW


Step 1: Data Ingestion Service reads 'customer_spending_1M_2018_2025.csv' from data/ folder
Step 2: Ingestion validates columns, cleans data, removes duplicates, handles missing values
Step 3: Data is partitioned by year/month and saved as Parquet files to data/raw/YYYY/
Step 4: Ingestion metadata is saved to data/processed/ingestion_metadata_*.json
Step 5: Spark ETL reads partitioned Parquet files from data/raw/
Step 6: Customer features are aggregated by segment and state with transaction metrics
Step 7: Quarterly features are created with time-series aggregations and growth rates
Step 8: Payment method features are generated showing distribution by segment
Step 9: State-level geographic features are calculated with top performers
Step 10: All features are saved to data/features/ as Parquet files
Step 11: Pipeline execution log is recorded in PostgreSQL for monitoring
Step 12: Features are ready for machine learning model training

================================================================================
                         AIRFLOW DAG WORKFLOWS

DAG 1: monthly_data_ingestion
  Schedule: 1st of every month at 00:00
  Tasks: Validate source data → Run ingestion → Validate ingestion → Notify completion
  Purpose: Automated monthly customer transaction data intake

DAG 2: quarterly_etl_feature_engineering
  Schedule: 1st of every 3rd month (quarterly) at 00:00
  Tasks: Check raw data → Spark ETL → Validate features → Create snapshot → Notify completion
  Purpose: Quarterly batch processing and feature generation

DAG 3: manual_full_pipeline
  Schedule: Manual trigger only (no automatic execution)
  Tasks: Run ingestion → Wait → Run Spark ETL → Generate report
  Purpose: For testing and on-demand full pipeline execution

DAG 4: data_quality_checks
  Schedule: Daily at 2:00 AM
  Tasks: Check null values → Check data freshness → Validate schema → Generate quality report
  Purpose: Continuous data quality monitoring and validation

================================================================================
                        USEFUL DOCKER COMMANDS

Step 1: View all running containers: docker-compose ps
Step 2: View logs from specific service (e.g., postgres): docker-compose logs postgres -f
Step 3: View logs from Airflow: docker-compose logs airflow -f
Step 4: View logs from Spark: docker-compose logs spark-master -f
Step 5: Stop all services: docker-compose down
Step 6: Stop all services and remove volumes: docker-compose down -v
Step 7: Restart a specific service: docker-compose restart postgres
Step 8: Execute command in running container: docker exec de-postgres psql -U dataeng -d feature_store -c "SELECT * FROM customer_features LIMIT 5;"
Step 9: View Docker images: docker images
Step 10: Prune unused Docker resources: docker system prune

================================================================================
                        DATABASE OPERATIONS


Step 1: Connect to PostgreSQL from your machine: psql -h localhost -U dataeng -d feature_store -p 5432 (password: dataeng123)
Step 2: List all tables in feature_store: \dt (when connected via psql)
Step 3: View customer features: SELECT * FROM customer_features LIMIT 10;
Step 4: View quarterly features: SELECT * FROM quarterly_features LIMIT 10;
Step 5: View pipeline execution logs: SELECT * FROM pipeline_execution_log ORDER BY execution_date DESC LIMIT 10;
Step 6: Count total ingested transactions: SELECT COUNT(*) FROM raw_transactions;
Step 7: Get revenue by state: SELECT state_names, total_revenue FROM state_features;
Step 8: Get revenue by segment: SELECT customer_segment, SUM(total_amount_spent) as revenue FROM customer_features GROUP BY customer_segment;
Step 9: View ML training features view: SELECT * FROM ml_training_features LIMIT 10;
Step 10: Exit PostgreSQL: \q


================================================================================
                        NEXT STEPS & FEATURES


Step 1: After features are generated, connect to PostgreSQL to explore customer_features table
Step 2: Review quarterly_features for time-series trends and growth rates
Step 3: Analyze payment_features to understand payment method preferences
Step 4: Use state_features to identify geographic revenue patterns
Step 5: Export features from PostgreSQL for machine learning model training
Step 6: Set up additional Airflow DAGs for model training and prediction pipelines
Step 7: Configure alerts and notifications for data quality issues
Step 8: Implement incremental ETL for continuous feature updates
Step 9: Add data visualization dashboards using tools like Grafana or Superset
Step 10: Scale the architecture using Kubernetes for production environments
