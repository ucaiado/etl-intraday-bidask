#!/bin/bash

# Create airflow.cfg file to use dag and plugin folders from this project
echo "Creating new airflow.cfg file..."
sed "s|OVERWRITEIT|$(pwd)|g" confs/airflow.template.cfg > airflow/airflow.cfg

echo "Exporting airflow home variable..."
export AIRFLOW_HOME=$(pwd)/airflow
airflow initdb
