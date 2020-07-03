# based on: https://bit.ly/2YGjuz6
import ast
import time

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class CustomEmrCreateJobFlowOperator(BaseOperator):
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            emr_conn_id='emr_default',
            template_files_path='',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.job_flow_overrides = {}

    def execute(self, context):
        # define hooks
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        emr = EmrHook(aws_conn_id=self.aws_conn_id,
                      emr_conn_id=self.emr_conn_id,
                      region_name='us-west-2')

        self.log.info('Defining JobFlow...')

        SPARK_STEPS = [
            {
                'Name': "copy-files-" + time.strftime("%Y%m%d-%H:%M"),
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ["aws", "s3", "cp",
                             f"s3://{Variable.get('S3_CODE_BUCKET_NAME')}/",
                             "/home/hadoop/", "--recursive"]
                }
            },
            {
                'Name': 'run-etl-' + time.strftime("%Y%m%d-%H:%M"),
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit', '/home/hadoop/spark_etl.py',
                        '-q', Variable.get('S3_RAW_QUOTES_BUCKET_NAME'),
                        '-op', Variable.get('S3_RAW_OPTIONS_BUCKET_NAME'),
                        '-d', context['ds_nodash'],
                        '-o', Variable.get('S3_DATA_BUCKET_NAME')
                    ]
                }
            }
        ]

        s_s32log = 's3n://aws-logs-345196100842-us-west-2/elasticmapreduce/'
        self.job_flow_overrides = {
            'Name': 'etl-process',
            'LogUri': s_s32log,
            'ReleaseLabel': 'emr-5.20.0',
            'Instances': {
                'InstanceGroups': [{
                    "InstanceCount": 2,
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [{
                            "VolumeSpecification": {
                                "SizeInGB": 32,
                                "VolumeType": "gp2"
                            },
                            "VolumesPerInstance": 1
                        }]
                    },
                    'Market': 'SPOT',
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
                    'Market': 'SPOT',
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "Name": "Master Instance Group"
                }],
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
            },
            'Steps': SPARK_STEPS,
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'ServiceRole': 'EMR_DefaultRole',
        }

        self.log.info('Creating JobFlow...')

        if isinstance(self.job_flow_overrides, str):
            self.job_flow_overrides = ast.literal_eval(self.job_flow_overrides)

        response = emr.create_job_flow(self.job_flow_overrides)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            self.log.info('JobFlow with id %s created', response['JobFlowId'])
            return response['JobFlowId']
