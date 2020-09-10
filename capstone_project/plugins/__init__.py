from airflow.plugins_manager import AirflowPlugin
from emr.custom_emr_add_step_operator import CustomAddStepsOperator
from emr.custom_emr_step_state_operator import CustomStepStateOperator
from s3.upload_file_operator import UploadFileToS3Operator
from athena.athena_quality_checks import AthenaQualityChecksOperator
from athena.custom_athena_operator import CustomAthenaOperator

class CustomEmrAddStepsPlugin(AirflowPlugin):
    name = "custom_emr_add_steps_plugin"
    operators = [CustomAddStepsOperator]

class CustomStepStatePlugin(AirflowPlugin):
    name = "custom_emr_step_state_plugin"
    operators = [CustomStepStateOperator]

class UploadFileToS3Plugin(AirflowPlugin):
    name = "upload_file_to_s3"
    operators = [UploadFileToS3Operator]


class AthenaQualityChecksPlugin(AirflowPlugin):
    name = "athena_quality_checks"
    operators = [AthenaQualityChecksOperator]

class CustomAthenaPlugin(AirflowPlugin):
    name = "athena_operator"
    operators = [CustomAthenaOperator]