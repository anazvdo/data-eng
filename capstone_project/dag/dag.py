from datetime import datetime, timedelta
import os
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.operators.custom_emr_add_steps_plugin import CustomEmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')
redshift_conn_id = Variable.get('redshift_conn_id')

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
      { 
         "Name": "copy-file",
         "ScriptBootstrapAction": { 
           "Path": "s3://scripts-emr/cp.sh"
         }
      }
   ],
    'Configurations': [
  {
     "Classification": "spark-env",
     "Configurations": [
       {
         "Classification": "export",
         "Properties": {
            "PYSPARK_PYTHON": "/usr/bin/python3"
          }
       }
    ]
  }
,
]
}

step_args = ["spark-submit --master yarn", 'anoca.py']

time = datetime.now()
step = [{"Name": "what_you_do-" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 's3n://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': step_args
            }
        }]

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

step_adder = CustomEmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='start_operator', key='return_value') }}",
    aws_conn_id='aws_udacity',
    steps=step,
    do_xcom_push=True,
    dag=dag
)

step_sensor = EmrStepSensor(
  task_id = 'step_sensor',
  job_flow_id="{{ task_instance.xcom_pull('start_operator', key='return_value') }}",
  step_id = "{{ task_instance.xcom_pull('step_adder', key='return_value')}}",
  aws_conn_id='aws_udacity',
  dag=dag
)



start_operator >> step_adder >> step_sensor
