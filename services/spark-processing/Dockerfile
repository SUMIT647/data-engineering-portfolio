FROM bitnami/spark:3.4.1

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    numpy==1.26.3 \
    psycopg2-binary==2.9.9 \
    pyarrow==14.0.2

# Download PostgreSQL JDBC driver to Spark jars directory
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/bitnami/spark/jars/postgresql-42.6.0.jar

# Create app directory
RUN mkdir -p /opt/spark-apps

# Copy Spark applications
COPY . /opt/spark-apps/

# Set working directory
WORKDIR /opt/spark-apps

USER 1001

USER 1001
