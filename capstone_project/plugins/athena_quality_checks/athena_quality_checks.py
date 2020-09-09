import boto3
import logging
from time import sleep
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
from botocore.exceptions import ClientError



class AthenaQualityChecksOperator(BaseOperator):
    template_fields=['query']
    template_ext = ('.sql',)

    ui_color = '#f699cd'
    @apply_defaults
    def __init__(self,
            aws_conn_id,
            region_name,
            output_location,
            database,
            query,
            expected_value,
            *args, **kwargs):
        super(AthenaQualityChecksOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.output_location = output_location
        self.database = database
        self.query = query
        self.expected_value = expected_value
    
    def create_client(self):
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

    def start_query(client, self):
        response = client.start_query_execution(
                            QueryString=self.query,
                            QueryExecutionContext={
                                'Database': self.database
                            },
                            ResultConfiguration={
                                'OutputLocation': self.output_location,
                            }
        )
        return response.get('QueryExecutionId')

    def execute(self, context):
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
        response = client.get_query_results(QueryExecutionId=query_id)

        result = response.get('ResultSet').get('Rows')[1].get('Data')[0].get('VarCharValue')

        if result != self.expected_value:
            raise ValueError(f'Error: Data Quality Check. Query:{self.query} Result:{result} Expected:{self.expected_value}')
        else:
            logging.info("Data Quaity OK")
        


# Defining the plugin class
class AthenaQualityChecksPlugin(AirflowPlugin):
    name = "athena_quality_checks"
    operators = [AthenaQualityChecksOperator]