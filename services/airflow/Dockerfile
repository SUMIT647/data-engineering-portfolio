FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.6.0 \
    pandas==2.1.4 \
    psycopg2-binary==2.9.9 \
    pyspark==3.5.0

# Set working directory
WORKDIR /opt/airflow

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh

USER root
RUN chmod +x /entrypoint.sh

USER airflow

ENTRYPOINT ["/entrypoint.sh"]
