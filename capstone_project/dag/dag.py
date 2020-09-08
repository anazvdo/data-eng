from datetime import datetime, timedelta
import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.operators.custom_emr_add_steps_plugin import CustomAddStepsOperator
from airflow.operators.custom_emr_step_state_plugin import CustomStepStateOperator
from airflow.contrib.operators.s3_copy_object_operator import S3CopyObjectOperator
from airflow.operators.upload_file_to_s3 import UploadFileToS3Operator

FILE_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'

default_args = {
    'owner': 'Ana Caroline Reis',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


JOB_FLOW_OVERRIDES = {
'LogUri': 's3://emr-logs-udacity/',
'Instances': {
    'Ec2KeyName': 'udacity',
    'InstanceGroups': [
        {

            'InstanceRole': 'MASTER',
            'InstanceType': 'm4.large',
            'InstanceCount': 1
        }
        
    ],
    'TerminationProtected': False,
    'KeepJobFlowAliveWhenNoSteps' : True
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    "Applications": [ 
      { 
         "Name": "spark" 
      },
      {
          "Name": "zeppelin"
      },
   ],
   'ReleaseLabel': "emr-5.30.1",
    'Name':'airflow-emr',
    'BootstrapActions': [ 
      
   ],
    'Configurations': [
  {
     "Classification": "spark-env",
     "Configurations": [
       {
         "Classification": "export",
         "Properties": {
            "PYSPARK_PYTHON": "/usr/bin/python3",
            
          }
       }
    ]
  }
,
]
}

step_args = ["spark-submit --master yarn", 'anoca.py']

time = datetime.now()
step = [
        {
            'Name': 'run-py-'+time.strftime('%Y%m%d_%H%M%S'),
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': '/var/lib/aws/emr/step-runner/hadoop-jars/command-runner.jar',
                'Args':  ["spark-submit", "s3://scripts-emr/etl.py"]
            }
        },
    ]

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 7 * * *',
          max_active_runs=1,
          catchup=False
        )

start_operator = EmrCreateJobFlowOperator(task_id='start_operator',  
                                dag=dag,
                                aws_conn_id='aws_udacity',
                                emr_conn_id='aws_emr',
                                job_flow_overrides=JOB_FLOW_OVERRIDES,
                                region_name='us-east-1')

copy_etl_to_s3 = UploadFileToS3Operator(task_id='copy_etl_to_s3',
                                        dag=dag,
                                        aws_conn_id='aws_udacity',
                                        local_path=FILE_PATH+'etl.py',
                                        region_name='us-east-1',
                                        s3_bucket_name='scripts-emr',
                                        s3_prefix='etl.py'
                )

step_adder = CustomAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='start_operator', key='return_value') }}",
    aws_conn_id='aws_udacity',
    region_name='us-east-1',
    steps=step,
    dag=dag
)

step_sensor = CustomStepStateOperator(
  task_id = 'step_sensor',
  job_flow_id="{{ task_instance.xcom_pull(task_ids='start_operator', key='return_value') }}",
  step_id = "{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')}}",
  aws_conn_id='aws_udacity',
  region_name='us-east-1',
  dag=dag
)



start_operator >> copy_etl_to_s3 >> step_adder >> step_sensor
