FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingestion scripts
COPY . .

# Create data directories
RUN mkdir -p /opt/data/raw /opt/data/processed

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DATA_PATH=/opt/data

CMD ["python", "ingest_data.py"]
