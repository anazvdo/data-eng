import boto3
import logging
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults



class CustomAddStepsOperator(BaseOperator):
    '''
    Operator to add steps to an existinf EMR job_flow
    Returns StepId
    :param aws_conn_id: Connection id of the aws connection to use
    :type aws_conn_id: str
    :param region_name: Region name of Athena tables
    :type region_name: str
    :param steps: boto3 style steps to be added to the jobflow. (templated)
    :type steps: list
    :param job_flow_id: id of the JobFlow to add steps to.  (templated)
    :type job_flow_id: str

    '''
    template_fields = ['job_flow_id', 'steps']
    ui_color = '#9400D3'
    @apply_defaults
    def __init__(self,
            aws_conn_id,
            region_name,
            steps,
            job_flow_id,
            *args, **kwargs):
        super(CustomAddStepsOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.region_name = region_name
        self.steps = steps
    
    def create_client(self):
        '''
        Create EMR Client
        '''
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
        '''
        Add steps to job flow id especified
        Return Step id
        '''
        client = self.create_client()
        response = client.add_job_flow_steps(
                    JobFlowId=self.job_flow_id,
                    Steps=self.steps)
        logging.info(response)

        return_value = response['StepIds'][0]
        return return_value
        


# Defining the plugin class
class CustomEmrAddStepsPlugin(AirflowPlugin):
    '''
        AddsSteps Plugin
    '''
    name = "custom_emr_add_steps_plugin"
    operators = [CustomAddStepsOperator]