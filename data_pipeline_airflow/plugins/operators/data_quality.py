from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    '''
    Operator that checks quality data, with a given sql query.

    :param redshift_conn_id: Airflow connection of a redshift cluster
    :type redshift_conn_id: str
    :param data_quality_checks: list of dicts containing sql query and the not expected value
    :type data_quality_checks: list of dicts
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 data_quality_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        self.log.info('Creating redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.data_quality_checks:
            sql = check.get('sql')
            not_expected = check.get('not_expected')

            result = redshift.get_records(sql)[0]
           
            got_error=False
            if not_expected == result[0]:
                got_error=True
                self.log.info('Error running: '+sql)

            if got_error:
                raise ValueError('Data Quality Check FAILED')
