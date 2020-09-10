import boto3
import logging
from time import sleep
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
from botocore.exceptions import ClientError



class CustomAthenaOperator(BaseOperator):
    '''
    Operator to run queries on Athena.
    
    :param aws_conn_id: Connection id of the aws connection to use
    :type aws_conn_id: str
    :param region_name: Region name of Athena tables
    :type region_name: str
    :param output_location: S3 path to save athena logs
    :type output_location: str
    :param database: Athena database
    :type database: str
    :param query: Presto to be run on athena. It must return 1 value(1 row and 1 column). (templated)
    :type query: str
    '''
    template_fields=['query']
    template_ext = ('.sql',)

    ui_color = '#3895d3'
    @apply_defaults
    def __init__(self,
            aws_conn_id,
            region_name,
            output_location,
            database,
            query,
            *args, **kwargs):
        super(CustomAthenaOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.output_location = output_location
        self.database = database
        self.query = query
    
    def create_client(self):
        '''
            Create Athena Client
        '''
        extras = BaseHook.get_connection(self.aws_conn_id).extra_dejson
        aws_session_token=''
        if len(extras) > 0:
            aws_session_token = extras['aws_session_token']
        key_id = BaseHook.get_connection(self.aws_conn_id).login
        secret_key = BaseHook.get_connection(self.aws_conn_id).password
        client = boto3.client('athena', aws_access_key_id=key_id,
                                    aws_secret_access_key=secret_key,
                                    aws_session_token=aws_session_token,
                                    region_name=self.region_name)
        logging.info("Athena Client is created")
        return client


    def execute(self, context):
        '''
            Execute Query and waits until its success.
            If query fails or is cancelled, this function raises erros.
        '''

        client = self.create_client()
        
        #Get query results
        response = client.start_query_execution(
                            QueryString=self.query,
                            QueryExecutionContext={
                                'Database': self.database
                            },
                            ResultConfiguration={
                                'OutputLocation': self.output_location,
                            }
        )
        query_id = response.get('QueryExecutionId')
        logging.info(query_id)
        response = client.get_query_execution(QueryExecutionId=query_id)
        status = response.get('QueryExecution').get('Status').get('State')
        logging.info('First Status: '+status)

        while status != 'SUCCEEDED':
            if status == 'CANCELLED': 
                raise ValueError('Query CANCELLED')
            elif status == 'FAILED':
                raise ValueError('Query FAILED')
            else:
                sleep(10)
                response = client.get_query_execution(QueryExecutionId=query_id)
                status = response.get('QueryExecution').get('Status').get('State')    
        logging.info("Query Succeeded")
        


class CustomAthenaPlugin(AirflowPlugin):
    '''
        Create Plugin
    '''
    name = "athena_operator"
    operators = [CustomAthenaOperator]