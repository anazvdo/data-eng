from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    '''
    Operator that inserts data into redshift dimension tables

    :param redshift_conn_id: Airflow connection of a redshift cluster
    :type redshift_conn_id: str
    :param table: table to insert data
    :type table: str
    :param query: query of data to insert
    :type query: str
    :param append: if data will be appended or not
    :type append: bool
    '''

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                 table,
                 query,
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.query = query

    def execute(self, context):
        self.log.info('Creating redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        table_insert = "INSERT INTO {}{}"
        formatted_sql = table_insert.format(
            self.table,
            self.query
        )

        self.log.info("Inserting data into dimension table "+formatted_sql)
        redshift.run(formatted_sql)
