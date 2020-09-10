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
from airflow.operators.athena_quality_checks import AthenaQualityChecksOperator
from airflow.operators.athena_operator import CustomAthenaOperator

FILE_PATH = os.path.dirname(os.path.realpath(__file__)) + '/'
DS_NODASH = '{{ ds_nodash }}'

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
		'KeepJobFlowAliveWhenNoSteps': False
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
	'Name': 'airflow-emr',
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
						"INPUT_JSON_PATH": "s3a://capstone-project-data/input_data/meta_Pet_Supplies.json",
						"INPUT_CSV_PATH": "s3a://capstone-project-data/input_data/ratings_Pet_Supplies.csv",
						"OUTPUT_S3_PATH": "s3://capstone-project-data/data_modeling/dimension_tables/",
                        "S3_PARTITION": "ymd="+DS_NODASH+"/"

					}
				}
			]
		},
	]
}

time = datetime.now()
STEP = [
	{
		'Name': 'run-py-'+time.strftime('%Y%m%d_%H%M%S'),
		'ActionOnFailure': 'TERMINATE_JOB_FLOW',
		'HadoopJarStep': {
				'Jar': '/var/lib/aws/emr/step-runner/hadoop-jars/command-runner.jar',
				'Args':  ["spark-submit", "s3://scripts-emr/etl.py"]
		}
	},
]


default_args = {
	'owner': 'Ana Caroline Reis',
	'start_date': datetime(2019, 1, 12),
	'depends_on_past': True,
	'retries': 2,
	'retry_delay': timedelta(minutes=5)
}

dag = DAG('datalake_amazon_pet_supplies_reviews',
		  default_args=default_args,
		  description='ETL Process using Pyspark on EMR to load Amazon Pet Supplies Data',
		  schedule_interval='0 7 * * *',
		  max_active_runs=1,
		  catchup=False
		  )
dag_start = DummyOperator(task_id="dag_start", dag=dag)

start_operator = EmrCreateJobFlowOperator(task_id='start_emr',
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
	task_id='add_steps_emr',
	job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr', key='return_value') }}",
	aws_conn_id='aws_udacity',
	region_name='us-east-1',
	steps=STEP,
	dag=dag
)

step_sensor = CustomStepStateOperator(
	task_id='step_sensor_emr',
	job_flow_id="{{ task_instance.xcom_pull(task_ids='start_emr', key='return_value') }}",
	step_id="{{ task_instance.xcom_pull(task_ids='add_steps_emr', key='return_value')}}",
	aws_conn_id='aws_udacity',
	region_name='us-east-1',
	dag=dag
)

dag_start >> start_operator >> copy_etl_to_s3 >> step_adder >> step_sensor

insert_fact_table = CustomAthenaOperator(task_id='insert_fact_table_product_ratings',
										dag=dag,
										query="./queries/fact_table.sql",
										database='pet_supplies',
										output_location='s3://capstone-project-data/athena_query_result_location/',
										aws_conn_id='aws_udacity',
										region_name='us-east-1'
									)

DIMENSION_TABLES = ['brand', 'category',
						'main_category', 'time', 'ratings', 'products']

for table in DIMENSION_TABLES:
	repair_fact_table = CustomAthenaOperator(task_id='repair_athena_fact_table_'+table,
											dag=dag,
											query='MSCK REPAIR TABLE '+ table,
											database='pet_supplies',
											output_location='s3://capstone-project-data/athena_query_result_location/',
											aws_conn_id='aws_udacity',
											region_name='us-east-1'
						)


	check_null_data = AthenaQualityChecksOperator(task_id='check_null_data_'+table,
													  dag=dag,
													  aws_conn_id='aws_udacity',
													  region_name='us-east-1',
													  output_location='s3://capstone-project-data/athena_query_result_location/',
													  database='pet_supplies',
													  query='./queries/data_quality/'+table+'_null_ids.sql',
													  expected_value='0')
	step_sensor >> repair_fact_table >> check_null_data
	if table != 'products':
		check_distinct_data = AthenaQualityChecksOperator(task_id='check_distinct_data_'+table,
													  dag=dag,
													  aws_conn_id='aws_udacity',
													  region_name='us-east-1',
													  output_location='s3://capstone-project-data/athena_query_result_location/',
													  database='pet_supplies',
													  query='./queries/data_quality/'+table+'_distinct.sql',
													  expected_value='true'
													  )
		check_null_data >> check_distinct_data >> insert_fact_table
	if table == 'products':
		step_sensor >> repair_fact_table >> check_null_data >> insert_fact_table

dag_end = DummyOperator(task_id="dag_end", dag=dag)

insert_fact_table >> dag_end
