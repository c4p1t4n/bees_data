#!/bin/bash

docker-compose down --volumes --remove-orphans                                          ✭ ✱
docker-compose up -d --build

# Wait for containers to be ready
echo "Waiting for containers to initialize..."
sleep 10

# Display access information
echo "Containers are up and running!"
echo "Access the services using the following URLs:"
echo "Airflow: http://localhost:8080"
echo "MinIO: http://localhost:9001"
echo ""
echo "Credentials:"
echo "Airflow - Username: admin | Password: admin"
echo "MinIO - Username: minio | Password: minio123"