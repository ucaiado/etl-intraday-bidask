#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Prepare data in S3 to be queried by Athena


@author: ucaiado

Created on 06/29/2020
"""

import datetime

from airflow import DAG
from airflow.models import Variable
from helpers import SqlQueries
from airflow.operators import (AthenaDataQuality, AthenaPartitionInsert)
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator


# Returns a DAG which creates a table if it does not exist, and then proceeds
# to load data into that table from S3. When the load is complete, a data
# quality  check is performed to assert that at least one row of data is
# present.
def get_s3_to_athena_dag(
        parent_dag_name,
        task_id,
        s3_key,
        d_sqls,
        *args, **kwargs):

    dag = DAG(f"{parent_dag_name}.{task_id}", **kwargs)

    s_s3 = f"s3://{Variable.get('S3_DATA_BUCKET_NAME')}/{s3_key}/"
    s_output_s3 = f"s3://{Variable.get('S3_CODES_BUCKET_NAME')}/"

    create_db = AWSAthenaOperator(
        task_id='Create_database',
        query=d_sqls['createdb'],
        output_location=s_output_s3,
        database='processed',
        aws_conn_id='aws_credentials',
        dag=dag
    )

    # drop_table = AWSAthenaOperator(
    #     task_id='Drop_table',
    #     query=d_sqls['drop'],
    #     output_location=s_output_s3,
    #     database='processed',
    #     aws_conn_id='aws_credentials',
    #     dag=dag
    # )

    create_table = AWSAthenaOperator(
        task_id='Create_table',
        query=d_sqls['create'].format(s_s3),
        output_location=s_output_s3,
        database='processed',
        aws_conn_id='aws_credentials',
        dag=dag
    )

    # bulk_insert_table = AWSAthenaOperator(
    #     task_id='Insert_data_on_table',
    #     query=d_sqls['load'],
    #     output_location=s_output_s3,
    #     database='processed',
    #     aws_conn_id='aws_credentials',
    #     dag=dag
    # )
    partition_insert_table = AthenaPartitionInsert(
        task_id='Insert_data_into_table',
        query=d_sqls['load2'],
        output_location=s_output_s3,
        database='processed',
        aws_conn_id='aws_credentials',
        dag=dag
    )

    s_sql = ("SELECT COUNT(*) "
             f"  FROM processed.{d_sqls['subkey']}"
             "  WHERE intdate > {}0001"
             "  AND intdate < {}2000")

    check_data_inserted = AthenaDataQuality(
        task_id='Fetch_data_from_table',
        query=s_sql,
        output_location=s_output_s3,
        database='processed',
        aws_conn_id='aws_credentials',
        dag=dag
    )

    # create_db >> drop_table >> create_table
    # create_table >> bulk_insert_table >> check_data_inserted
    create_db >> create_table >> partition_insert_table
    partition_insert_table >> check_data_inserted

    return dag
