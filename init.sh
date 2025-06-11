#!/bin/bash

# Define permissões
chmod -R 755 docker/dags/
chmod -R 755 docker/src/
chmod -R 777 docker/logs/

# Sobe os containers
cd docker/
docker-compose down --volumes --remove-orphans
docker-compose up -d --build

# Espera os containers iniciarem
echo "Waiting for containers to initialize..."
sleep 15

# Informações de acesso
echo "Containers are up and running!"
echo "Access the services using the following URLs:"
echo "Airflow: http://localhost:8080"
echo "MinIO: http://localhost:9001"
echo ""
echo "Credentials:"
echo "Airflow - Username: admin | Password: admin"
echo "MinIO - Username: airflowuser | Password: airflowpass123"
