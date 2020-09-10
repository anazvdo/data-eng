import boto3
import logging
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
from botocore.exceptions import ClientError



class UploadFileToS3Operator(BaseOperator):
    '''
    Operator to Upload local file to S3 path
    type aws_conn_id: str
    :param region_name: Region name of Athena tables
    :type region_name: str
    :param local_path: local path where is the file to be uploaded
    :type local_path: str
    :param s3_bucket_name: name of s3 bucket
    :type s3_bucket_name: str
    :param s3_prefix: prefix of s3 path
    :type s3_prefix: str
    '''
    ui_color = '#f47321'
    @apply_defaults
    def __init__(self,
            aws_conn_id,
            region_name,
            local_path,
            s3_bucket_name,
            s3_prefix,
            *args, **kwargs):
        super(UploadFileToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.local_path = local_path
        self.s3_bucket_name = s3_bucket_name
        self.s3_prefix = s3_prefix
    
    def create_client(self):
        '''
        Create S3 Client
        '''
        extras = BaseHook.get_connection(self.aws_conn_id).extra_dejson
        aws_session_token=''
        if len(extras) > 0:
            aws_session_token = extras['aws_session_token']
        key_id = BaseHook.get_connection(self.aws_conn_id).login
        secret_key = BaseHook.get_connection(self.aws_conn_id).password
        client = boto3.client('s3', aws_access_key_id=key_id,
                                    aws_secret_access_key=secret_key,
                                    aws_session_token=aws_session_token,
                                    region_name=self.region_name)
        logging.info("S3 Client is created")
        return client

    def execute(self, context):
        '''
        Upload file
        '''
        client = self.create_client()
        # Upload the file
        try:
            logging.info(self.local_path)
            logging.info(self.s3_bucket_name)
            logging.info(self.s3_prefix)
            response = client.upload_file(self.local_path, self.s3_bucket_name, self.s3_prefix)
        except ClientError as e:
            logging.error(e)
            raise ValueError
        return True
        


# Defining the plugin class
class UploadFileToS3Plugin(AirflowPlugin):
    '''
    Upload File Plugin
    '''
    name = "upload_file_to_s3"
    operators = [UploadFileToS3Operator]