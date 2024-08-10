#!/bin/bash

# Start the Airflow scheduler and webserver
airflow scheduler &
airflow webserver -p 8080
