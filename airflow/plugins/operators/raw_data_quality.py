import glob
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class RawDataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    template_fields = ('aws_conn_id', 'dest_bucket_name')

    @apply_defaults
    def __init__(self,
                 aws_conn_id='',
                 dest_bucket_name='',
                 files_path='',
                 *args, **kwargs):

        super(RawDataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.dest_bucket_name = dest_bucket_name
        self.files_path = files_path

    def execute(self, context):

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info(f'Check files in S3 and in local folder ...')
        s_local_filespath = f"{self.files_path}/{context['ds_nodash']}*.csv"
        l_local_files = glob.glob(s_local_filespath)
        l_files_in_s3 = s3_hook.list_keys(self.dest_bucket_name)

        if all(elem in l_files_in_s3 for elem in l_local_files):
            if len(l_local_files) > 0:
                raise ValueError(f"Not all elements in the path "
                                 f"{s_local_filespath} were found in S3"
                                 f" {self.dest_bucket_name} bucket")
        self.log.info(f"All {len(l_local_files)}  in local folder are in "
                      f" {self.dest_bucket_name} bucket")
