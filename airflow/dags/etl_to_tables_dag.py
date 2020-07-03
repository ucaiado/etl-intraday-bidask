#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Tranform raw data and save back to S3

@author: ucaiado

Created on 06/26/2020
"""

from datetime import datetime, timedelta
import os
import time
import glob
import logging
from airflow import DAG
from helpers import SqlQueries
from athena_subdag import get_s3_to_athena_dag
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (S3UploadOperator, RawDataQualityOperator,
                               CustomEmrCreateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.s3_bucket import (
    S3CreateBucketOperator, S3DeleteBucketOperator)


# source: https://airflow.apache.org/docs/stable/tutorial.html
DEFAULT_ARGS = {
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


def return_branch(**context):
    # source: https://www.astronomer.io/guides/airflow-branch-operator/
    # source: https://bit.ly/38ggwVf

    for s_prefix in ['quotes', 'options']:
        s_file_path = f"{Variable.get('LOCAL_FILES_PATH')}/{s_prefix}"
        s_local_filespath = f"{s_file_path}/{context['ds_nodash']}*.csv"
        l_local_files = glob.glob(s_local_filespath)
        if len(l_local_files) == 0:
            logging.info(f"!! stop execution because there is no file to "
                         f"handle after filtering {s_local_filespath}")
            return 'Stop_execution'
        logging.info(f"!! continue because there is {len(l_local_files)} files"
                     f" to handle after filtering {s_local_filespath}")

    return 'Wrangling_on_EMR'


# source: https://bit.ly/2BfWxdd

dag = DAG('create_datalake',
          default_args=DEFAULT_ARGS,
          description='Upload data to S3 with Airflow',
          schedule_interval='@daily',
          max_active_runs=3
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# create buckets

create_s3_bucket2misc = S3CreateBucketOperator(
    task_id='Create_misc_bucket',
    bucket_name=Variable.get('S3_CODE_BUCKET_NAME'),
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )

create_s3_bucket2data = S3CreateBucketOperator(
    task_id='Create_output_bucket',
    bucket_name=Variable.get('S3_DATA_BUCKET_NAME'),
    aws_conn_id='aws_credentials',
    region_name='us-west-2',
    dag=dag
    )


# upload etl code to be used by emr

upload_code2s3 = S3UploadOperator(
    task_id='Upload_etl_codes_to_s3',
    aws_credentials_id='aws_credentials',
    dest_bucket_name=Variable.get('S3_CODES_BUCKET_NAME'),
    singlefile_path=Variable.get('LOCAL_FILES_PATH').replace(
        'data', 'airflow/dags/spark_etl.py'),
    dag=dag
    )


# branching after checks

counting_files = BranchPythonOperator(
        task_id='Counting_files',
        python_callable=return_branch,
        provide_context=True,
        trigger_rule="all_done",
        dag=dag)


# cerate emr cluster

instantiate_emr = CustomEmrCreateJobFlowOperator(
    task_id='Wrangling_on_EMR',
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    dag=dag
    )

s_job_flow_id = "{{ task_instance.xcom_pull(task_ids='Wrangling_on_EMR', "
s_job_flow_id += "key='return_value') }}"
monitor_emr = EmrJobFlowSensor(
    task_id='Monitor_cluster',
    job_flow_id=s_job_flow_id,
    aws_conn_id='aws_credentials',
    dag=dag
    )


# update athena

athena_task_id = "Put_quotes_on_athena"
d_sqls = {
    'subkey': 'quotes',
    'createdb': SqlQueries.quotes_create_database,
    'drop': SqlQueries.quotes_drop_table,
    'create': SqlQueries.quotes_create_table,
    'load': SqlQueries.quotes_rapair,
    'load2': SqlQueries.quotes_load
}

update_quotes_on_athena = SubDagOperator(
    subdag=get_s3_to_athena_dag(
        "create_datalake",
        athena_task_id,
        s3_key="quotes.parquet",
        d_sqls=d_sqls,
        start_date=datetime.utcnow(),
    ),
    task_id=athena_task_id,
    dag=dag,
)


athena_task_id = "Put_options_on_athena"
d_sqls = {
    'subkey': 'options',
    'createdb': SqlQueries.options_create_database,
    'drop': SqlQueries.options_drop_table,
    'create': SqlQueries.options_create_table,
    'load': SqlQueries.options_rapair,
    'load2': SqlQueries.options_load
}

update_options_on_athena = SubDagOperator(
    subdag=get_s3_to_athena_dag(
        "create_datalake",
        athena_task_id,
        s3_key="options.parquet",
        d_sqls=d_sqls,
        start_date=datetime.utcnow(),
    ),
    task_id=athena_task_id,
    dag=dag,
)


# check data quality


end_operator = DummyOperator(
    task_id='Stop_execution',
    trigger_rule='none_failed',
    dag=dag)


# DAGs dependencies
# source: https://www.astronomer.io/guides/managing-dependencies/

start_operator >> [create_s3_bucket2misc, create_s3_bucket2data]
[create_s3_bucket2misc, create_s3_bucket2data] >> upload_code2s3
upload_code2s3 >> counting_files
counting_files >> instantiate_emr
counting_files >> end_operator
instantiate_emr >> monitor_emr
monitor_emr >> [update_quotes_on_athena, update_options_on_athena]
[update_quotes_on_athena, update_options_on_athena] >> end_operator
