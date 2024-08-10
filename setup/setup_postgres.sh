#!/bin/bash

# Run this script to setup the PostgreSQL database

docker-compose up -d postgres

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# Run the database initialization
docker-compose exec postgres psql -U root -d crawl -c "CREATE TABLE IF NOT EXISTS urls (id serial PRIMARY KEY, url TEXT);"
