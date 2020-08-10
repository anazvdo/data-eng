from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'  

    '''
    Operator that inserts data into redshift fact table

    :param redshift_conn_id: Airflow connection of a redshift cluster
    :type redshift_conn_id: str
    :param table: table to insert data
    :type table: str
    :param query: query of data to insert
    :type query: str
    :param columns: columns to insert data
    :type columns: string of columns delimited by ','
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 query,
                 columns,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table=table
        self.query = query
        self.columns = columns

    def execute(self, context):
        self.log.info('Creating redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        table_insert = "INSERT INTO {}({}){}"
        formatted_sql = table_insert.format(
            self.table,
            self.columns,
            self.query
        )

        self.log.info("Inserting data into fact table"+formatted_sql)
        redshift.run(formatted_sql)