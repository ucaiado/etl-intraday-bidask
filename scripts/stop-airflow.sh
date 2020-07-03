#!/bin/bash

echo "Stopping servers..."
pkill airflow-webserver
pkill airflow
echo "Done!"
