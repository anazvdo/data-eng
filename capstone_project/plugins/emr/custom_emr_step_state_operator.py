import boto3
import logging
from time import sleep
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults



class CustomStepStateOperator(BaseOperator):
    template_fields = ['job_flow_id', 'step_id']
    ui_color = '#40e0d0'
    @apply_defaults
    def __init__(self,
            aws_conn_id,
            region_name,
            step_id,
            job_flow_id,
            *args, **kwargs):
        super(CustomStepStateOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.region_name = region_name
        self.step_id = step_id
    
    def create_client(self):
        extras = BaseHook.get_connection(self.aws_conn_id).extra_dejson
        aws_session_token=''
        if len(extras) > 0:
            aws_session_token = extras['aws_session_token']
        key_id = BaseHook.get_connection(self.aws_conn_id).login
        secret_key = BaseHook.get_connection(self.aws_conn_id).password
        client = boto3.client('emr', aws_access_key_id=key_id,
                                    aws_secret_access_key=secret_key,
                                    aws_session_token=aws_session_token,
                                    region_name=self.region_name)
        logging.info("EMR Client is created")
        return client

    def execute(self, context):
        client = self.create_client()
        step_state=''

        while step_state != 'COMPLETED':
            response = client.describe_step(
                ClusterId=self.job_flow_id,
                StepId=self.step_id
            )    

            step_id = response['Step']['Id']
            step_name = response['Step']['Name']
            step_state = response['Step']['Status']['State']

            print(f'State of step {step_name} on cluster j-1N2F5S9K2WCHB: {step_state}')
            if step_state not in ('RUNNING', 'COMPLETED', 'PENDING'):
                raise ValueError(f'Error in step {step_name}: {step_state}')
            sleep(10)
                        


# Defining the plugin class
class CustomStepStatePlugin(AirflowPlugin):
    name = "custom_emr_step_state_plugin"
    operators = [CustomStepStateOperator]