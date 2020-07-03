#!/bin/bash

# Start airflow
airflow initdb
airflow scheduler --daemon
airflow webserver --daemon -p 3000

# Set variables to airflow use
# airflow variables --import confs/airflow-variables.json

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
  _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done
