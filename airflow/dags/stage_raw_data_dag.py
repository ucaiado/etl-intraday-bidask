#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Upload data stored locally to S3 buckets

@author: ucaiado

Created on 06/26/2020
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3UploadOperator, RawDataQualityOperator)
from airflow.providers.amazon.aws.operators.s3_bucket import (
    S3CreateBucketOperator, S3DeleteBucketOperator)


# source: https://airflow.apache.org/docs/stable/tutorial.html
default_args = {
    'owner': 'ucaiado',
    'start_date': datetime(2020, 4, 1),
    'end_date': datetime(2020, 4, 11),
    # The DAG does not have dependencies on past runs
    'depends_on_past': False,
    # On failure, the task are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False,
    # Do not email on retry
    'email_on_retry': False

}

dag = DAG('upload_raw_data',
          default_args=default_args,
          description='Upload data to S3 with Airflow',
          schedule_interval='@daily',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# create buckets

create_s3_bucket2quotes = S3CreateBucketOperator(
    task_id='Create_quotes_bucket',
    bucket_name=Variable.get('S3_RAW_QUOTES_BUCKET_NAME'),
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )

create_s3_bucket2options = S3CreateBucketOperator(
    task_id='Create_options_bucket',
    bucket_name=Variable.get('S3_RAW_OPTIONS_BUCKET_NAME'),
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )


# upload daily data

upload_quotes2s3 = S3UploadOperator(
    task_id='Upload_quotes_to_s3',
    aws_credentials_id='aws_credentials',
    dest_bucket_name=Variable.get('S3_RAW_QUOTES_BUCKET_NAME'),
    files_path=Variable.get('LOCAL_FILES_PATH') + '/quotes',
    dag=dag
    )

upload_options2s3 = S3UploadOperator(
    task_id='Upload_options_to_s3',
    aws_credentials_id='aws_credentials',
    dest_bucket_name=Variable.get('S3_RAW_OPTIONS_BUCKET_NAME'),
    files_path=Variable.get('LOCAL_FILES_PATH') + '/options',
    dag=dag
    )


# check data quality

check_quotes_in_s3 = RawDataQualityOperator(
    task_id='Check_quotes_in_s3',
    aws_credentials_id='aws_credentials',
    dest_bucket_name=Variable.get('S3_RAW_QUOTES_BUCKET_NAME'),
    files_path=Variable.get('LOCAL_FILES_PATH') + '/quotes',
    dag=dag
    )

check_options_in_s3 = RawDataQualityOperator(
    task_id='Check_options_in_s3',
    aws_credentials_id='aws_credentials',
    dest_bucket_name=Variable.get('S3_RAW_OPTIONS_BUCKET_NAME'),
    files_path=Variable.get('LOCAL_FILES_PATH') + '/options',
    dag=dag
    )


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# DAGs dependencies
# source: https://www.astronomer.io/guides/managing-dependencies/

start_operator >> [create_s3_bucket2quotes, create_s3_bucket2options]
[create_s3_bucket2quotes, create_s3_bucket2options] >> upload_quotes2s3
create_s3_bucket2quotes >> upload_quotes2s3
create_s3_bucket2options >> upload_options2s3
upload_quotes2s3 >> check_quotes_in_s3
upload_options2s3 >> check_options_in_s3
[check_quotes_in_s3, check_options_in_s3] >> end_operator
