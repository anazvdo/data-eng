from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''
    Operator that copies data from S3 bucket to redshift staging tables

    :param redshift_conn_id: Airflow connection of a redshift cluster
    :type redshift_conn_id: str
    :param aws_key: AWS_ACCESS_KEY_ID
    :type aws_key: str
    :param aws_secret: AWS_SECRET_ACCESS_KEY
    :type aws_secret: str
    :param table: table to insert data
    :type table: str
    :param s3_path: s3 path (s3://bucket/prefix/)
    :type s3_path: str
    :param region: columns to insert data
    :type region: region of s3 bucket
    :param json_meta: auto to infer metadata or json path
    :type json_meta: str
    '''


    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
        COMPUPDATE OFF
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id, 
                 aws_key,
                 aws_secret,
                 table,
                 s3_path,
                 region,
                 json_meta="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.table = table
        self.s3_path = s3_path
        self.region = region
        self.json_meta = json_meta

    def execute(self, context):
        self.log.info('Creating redshift connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        
        formatted_sql = self.copy_sql.format(
            self.table,
            self.s3_path,
            self.aws_key,
            self.aws_secret,
            self.region,
            self.json_meta
        )
        self.log.info("Copying data from S3 to Redshift"+formatted_sql)
        redshift.run(formatted_sql)
        

