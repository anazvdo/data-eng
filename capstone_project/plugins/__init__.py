from airflow.plugins_manager import AirflowPlugin
from emr.custom_emr_add_step_operator import CustomAddStepsOperator
from emr.custom_emr_step_state_operator import CustomStepStateOperator

class CustomEmrAddStepsPlugin(AirflowPlugin):
    name = "custom_emr_add_steps_plugin"
    operators = [CustomAddStepsOperator]

class CustomStepStatePlugin(AirflowPlugin):
    name = "custom_emr_step_state_plugin"
    operators = [CustomStepStateOperator]