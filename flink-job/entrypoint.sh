#!/bin/bash

echo "========================================="
echo "Flink Job Submitter - Full Star Schema"
echo "========================================="

echo "Waiting for Kafka to be ready..."
for i in {1..30}; do
  if kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; then
    echo "Kafka is ready"
    break
  fi
  echo "Attempt $i/30: Kafka not ready yet..."
  sleep 2
done

echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
  if PGPASSWORD=flink_password psql -h postgres -U flink_user -d star_schema -c "SELECT 1" > /dev/null 2>&1; then
    echo "PostgreSQL is ready"
    break
  fi
  echo "Attempt $i/30: PostgreSQL not ready yet..."
  sleep 2
done

echo "Starting Full Star Schema streaming job..."
python3 /app/flink_job.py