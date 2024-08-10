#!/bin/bash

# Run this script to setup the Airflow environment

docker-compose up -d airflow

# Initialize the Airflow database
echo "Initializing db"
docker-compose exec airflow airflow db init

# Create an Airflow user
docker-compose exec airflow airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com


echo "Starting"
# Start Airflow webserver and scheduler
docker-compose exec airflow airflow webserver -p 8080 &
docker-compose exec airflow airflow scheduler &
