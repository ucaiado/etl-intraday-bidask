import glob
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults


class ProcessedDataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    template_fields = ('aws_conn_id', 'dest_bucket_name')

    @apply_defaults
    def __init__(self,
                 aws_conn_id='',
                 dest_bucket_name='',
                 prefix_key='',
                 *args, **kwargs):

        super(ProcessedDataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.dest_bucket_name = dest_bucket_name
        self.prefix_key = prefix_key

    def execute(self, context):

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s_date = str(context['ds_nodash'])
        s_year, s_month, s_day = s_date[:4], s_date[4:6], s_date[6:]

        s_path2s3 = f'{self.dest_bucket_name}/{self.prefix_key}'
        s_path2s3 += f'year={s_year}/month={s_month}/day{s_day}/'
        self.log.info(f'Check files in {s_path2s3} bucket ...')
        l_files_in_s3 = s3_hook.list_keys(s_path2s3)

        if len(l_files_in_s3) == 0:
            raise ValueError(f"No files created after ETL ...")
        self.log.info(f"There are some files in the related date ...")
