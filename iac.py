#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
'Infrastructure as Code'


@author: udacity, ucaiado

Created on 07/07/2020
"""

# import libraries
import argparse
import textwrap
import boto3
import json
import os
import glob
import tqdm
import configparser
import subprocess
import pathlib
import pandas as pd
from rich.console import Console
from rich.table import Column, Table
from rich.progress import track


'''
Begin help functions and variables
'''

config = configparser.ConfigParser()
config.read_file(open('confs/dpipe.cfg'))


KEY = config.get('AWS', 'ACCESS_KEY_ID')
SECRET = config.get('AWS', 'SECRET_ACCESS_KEY')
DL_CLUSTER_ID = config.get("CLUSTER", "ID")
IAM_ROLE_ARN = config.get("IAM_ROLE",  "ARN")
DL_IAM_ROLE_NAME = config.get("CLUSTER", "DL_IAM_ROLE_NAME")
DL_QUOTES_BUCKET_NAME = config.get("CLUSTER", "DL_QUOTES_BUCKET_NAME")
DL_OPTIONS_BUCKET_NAME = config.get("CLUSTER", "DL_OPTIONS_BUCKET_NAME")
DL_CODE_BUCKET_NAME = config.get("CLUSTER", "DL_CODE_BUCKET_NAME")
DL_DATA_BUCKET_NAME = config.get("CLUSTER", "DL_DATA_BUCKET_NAME")


def prettyEMRProps(props):
    l_data = [('Id', props['Id']),
              ('Name', props['Name']),
              ('State', props['Status']['State'])]

    console = Console()

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Key", style="dim", width=12)
    table.add_column("Value", justify="right")
    for row in l_data:
        table.add_row(
            *row
        )

    console.print(table)


def upload_files(s3, bucket_name, l_filepaths):
    # source: https://go.aws/3dvMlLb
    try:
        response = s3.list_buckets()
        l_buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name not in l_buckets:
            d_bucket_conf = {'LocationConstraint': 'us-west-2'}
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=d_bucket_conf)
            print(f'...create a new bucket with the name {bucket_name}')
        for s_filepath in track(l_filepaths):
            s_fname = s_filepath.split('/')[-1]
            s3.upload_file(s_filepath, bucket_name, s_fname)
    except Exception as e:
        print(e)
        return False
    return True


'''
End help functions and variables
'''


if __name__ == '__main__':
    s_txt = '''\
            Infrastructure as code
            --------------------------------
            Create Amazon EMR cluster and setup Airflow variables
            '''
    # include and parse variables
    obj_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=obj_formatter, description=textwrap.dedent(s_txt))

    s_help = 'Create IAM role'
    parser.add_argument('-i', '--iam', action='store_true', help=s_help)

    s_help = 'Upload data to S3'
    parser.add_argument('-u', '--upload', action='store_true', help=s_help)

    s_help = 'Create a EMR cluster'
    parser.add_argument('-e', '--emr', action='store_true', help=s_help)

    s_help = 'Check EMR cluster status'
    parser.add_argument('-s', '--status', action='store_true', help=s_help)

    s_help = 'Run etl.py'
    parser.add_argument('-r', '--run', action='store_true', help=s_help)

    s_help = 'Create Airflow Variable file'
    parser.add_argument('-a', '--airflow', action='store_true', help=s_help)

    s_help = 'Clean up your specified resources'
    parser.add_argument('-d', '--delete', action='store_true', help=s_help)

    s_help = '!!WARN: Clean up ALL your resources'
    parser.add_argument('-da', '--deleteall', action='store_true', help=s_help)

    # check what should do
    args = parser.parse_args()
    b_create_iam = args.iam
    b_create_emr = args.emr
    b_check_status = args.status
    b_delete = args.delete
    b_deletea = args.deleteall
    b_upload = args.upload
    b_airflow = args.airflow

    # check the step selected
    s_err = 'Please select one, and only one, option from -h menu'
    i_test_all = (b_create_iam*1 + b_create_emr*1 + b_check_status*1 +
                  b_delete*1 + b_deletea*1 + b_upload*1 + b_airflow*1)
    assert i_test_all == 1, s_err
    if b_create_emr:  # or b_open_tcp:
        assert len(IAM_ROLE_ARN) > 2, 'Please run --iam flag before this step'

    # create clients
    print('...create clients for S3, IAM, and EMR')
    s3 = boto3.client(
        's3',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    iam = boto3.client(
        'iam',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    emr = boto3.client(
        'emr',
        region_name='us-west-2',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET)

    if b_create_iam:
        # Create the IAM role
        try:
            dl_role = iam.create_role(
                Path='/',
                RoleName=DL_IAM_ROLE_NAME,
                Description=('Allows EMR clusters to call AWS '
                             'services on your behalf'),
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'elasticmapreduce.amazonaws.com'}}],
                     'Version': '2012-10-17'}
                ))
            print('...create a new IAM Role')
        except Exception as e:
            print(e)

        # Attaching Policy
        print('...attach policy')
        iam.attach_role_policy(
            RoleName=DL_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        # Get and print the IAM role ARN
        print('...get the IAM role ARN')
        role_arn = iam.get_role(RoleName=DL_IAM_ROLE_NAME)['Role']['Arn']
        print('   !! fill in the IAM_ROLE ARN field in dpipe.cfg file with the '
              'following string:')
        print(role_arn)

    elif b_create_emr:
        try:
            d_cluster_confs = {
                'Name': 'spark-udacity',
                'LogUri': 's3n://aws-logs-345196100842-us-west-2/elasticmapreduce/',
                'ReleaseLabel': 'emr-5.20.0',
                'Applications': [
                    {'Name': 'Ganglia'},
                    {'Name': 'Spark'},
                    {'Name': 'Zeppelin'}
                    ],
                'Instances': {
                    'InstanceGroups': [{
                        "InstanceCount": 3,
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [{
                                "VolumeSpecification": {
                                    "SizeInGB": 32,
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }]
                        },
                        'Market': 'ON_DEMAND',
                        "InstanceRole": "CORE",
                        "InstanceType": "m5.xlarge",
                        "Name": "Core Instance Group"},
                        {
                        "InstanceCount": 1,
                        "EbsConfiguration": {
                            "EbsBlockDeviceConfigs": [{
                                "VolumeSpecification": {
                                    "SizeInGB": 32,
                                    "VolumeType": "gp2"
                                },
                                "VolumesPerInstance": 1
                            }]
                        },
                        'Market': 'ON_DEMAND',
                        "InstanceRole": "MASTER",
                        "InstanceType": "m5.xlarge",
                        "Name": "Master Instance Group"
                    }],
                    'EmrManagedMasterSecurityGroup': 'sg-026b87da127016424',
                    'EmrManagedSlaveSecurityGroup': 'sg-0dd6822241f2f3849',
                    'Ec2KeyName': 'spark-cluster',
                    'Ec2SubnetId': 'subnet-0a7af642',
                    "KeepJobFlowAliveWhenNoSteps": True,
                    'TerminationProtected': False
                },
                'VisibleToAllUsers': True,
                'EbsRootVolumeSize': 10,
                'JobFlowRole': 'EMR_EC2_DefaultRole',
                'ServiceRole': 'EMR_DefaultRole',
                'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION'
            }

            response = emr.run_job_flow(**d_cluster_confs)
            print('   !! fill in the CLUSTER ID field in dpipe.cfg file '
                  'with the following string:')
            print("CLUSTER ID :: ", response['JobFlowId'])
        except Exception as e:
            print(e)

    elif b_check_status:
        print('...check cluster status')
        try:
            my_cluster_prop = emr.describe_cluster(
                ClusterId=DL_CLUSTER_ID)['Cluster']
            prettyEMRProps(my_cluster_prop)
            print('\n')
        except Exception as e:
            print(e)

    elif b_delete:
        print('...clean up your resources')
        response = emr.list_clusters(ClusterStates=['WAITING'])
        ii = 0
        for ii, cluster in enumerate(response['Clusters']):
            pass
        cluster_id = DL_CLUSTER_ID
        print(f"!!terminating cluster {cluster_id}")
        response2 = emr.terminate_job_flows(JobFlowIds=[cluster_id])
        my_cluster_prop = emr.describe_cluster(
            ClusterId=DL_CLUSTER_ID)['Cluster']
        prettyEMRProps(my_cluster_prop)
        print('\n')
        if ii > 1:
            print(' !! THERE IS MORE THAN 1 CLUSTER AVAILABLE')

    elif b_deletea:
        print('...clean up your resources')
        response = emr.list_clusters(ClusterStates=['WAITING'])
        ii = 0
        for ii, cluster in enumerate(response['Clusters']):
            cluster_id = cluster['Id']
            cluster_name = cluster['Name']
            print(f"!!terminating cluster {cluster_id}")
            response2 = emr.terminate_job_flows(JobFlowIds=[cluster_id])
            my_cluster_prop = emr.describe_cluster(
                ClusterId=DL_CLUSTER_ID)['Cluster']
            prettyEMRProps(my_cluster_prop)
            print('\n')
        if ii == 0:
            print('...no clusters with "WAITING status to terminate')

    elif b_upload:
        # NOTE: include wget to download files somewhere
        print('...uploading files from QUOTES to S3 bucket')
        b_test1 = upload_files(s3, DL_QUOTES_BUCKET_NAME, glob.glob(
            'data/quotes/*.csv'))
        print('...uploading files from OPTIONS to S3 bucket')
        b_test2 = upload_files(s3, DL_OPTIONS_BUCKET_NAME, glob.glob(
            'data/options/*.csv'))
        if b_test1 and b_test2:
            print('...finished uploading files to S3 bucket')

    elif b_airflow:
        from airflow import settings
        from airflow.models.connection import Connection
        from airflow.models import Variable

        print('...delete old airflow connections')
        s_bash = f"airflow connections -d --conn_id aws_credentials "
        process = subprocess.Popen(s_bash.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()

        print('...create airflow connection to AWS')

        c = Connection(
            conn_id='aws_credentials',
            conn_type='aws',
            login=f'{KEY}',
            password=f'{SECRET}')
        session = settings.Session()
        session.add(c)
        session.commit()

        print('...create variables to store S3 paths')
        s_data_parh = str(pathlib.Path().absolute() / 'data')
        Variable.set("S3_RAW_QUOTES_BUCKET_NAME", DL_QUOTES_BUCKET_NAME)
        Variable.set("S3_RAW_OPTIONS_BUCKET_NAME", DL_OPTIONS_BUCKET_NAME)
        Variable.set("S3_DATA_BUCKET_NAME", DL_DATA_BUCKET_NAME)
        Variable.set("S3_CODES_BUCKET_NAME", DL_CODE_BUCKET_NAME)
        Variable.set("LOCAL_FILES_PATH", s_data_parh)
