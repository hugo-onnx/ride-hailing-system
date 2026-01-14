FROM python:3.12-slim

WORKDIR /app

# Install system dependencies required for confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    confluent-kafka>=2.13.0 \
    redis>=7.1.0

# Copy application code
COPY services/common /app/common
COPY services/feature-consumer/main.py /app/

ENV PYTHONPATH=/app

CMD ["python", "main.py"]