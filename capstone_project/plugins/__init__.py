from airflow.plugins_manager import AirflowPlugin
from emr.custom_emr_add_step_operator import CustomAddStepsOperator

class CustomEmrAddStepsPlugin(AirflowPlugin):
    name = "custom_emr_add_steps_plugin"
    operators = [CustomAddStepsOperator]