from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageTablesOperator(BaseOperator):
    """
        @description:
            This operator copies data from a specified S3 bucket to Amazons Redshift.
        @params:
            redshift_conn_id (STR): Redshift Connection ID created in Airflow.
            aws_connection_id (STR): AWS connection ID created in Airflow.
            table (STR): The name of the table the data in S3 should be copied to.
            s3_bucket (STR): Created S3 bucket name
            s3_key (STR): Folder in the S3 bucket that contains data to be transfered to Redshift
            ignpre_headers (INT): Specifies if these dataset contain headers. 1 for True. 0 for False
            delimeter (CHAR): Dataset separator. Aplicable with files in CSV format.
            data_format (STR): Format the data is saved in S3. E.g. CSV, PARQUET, JSON.   
    """
    def __init__(
        self,
        redshift_conn_id = "redshift_conn_id",
        aws_connection_id = "aws_conn_id",
        table = "",
        s3_bucket = "",
        s3_key = "",
        ignore_headers = 1,
        delimeter = ",",
        data_format = "csv",
        *args, **kwargs
        ):
        super(StageTablesOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_connection_id = aws_connection_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.delimeter = delimeter
        self.data_format = data_format

    def execute(self, context):
        self.log.info("Fetching credentials")
        aws_hook = AwsHook(self.aws_connection_id, client_type='s3')
        aws_credentials = aws_hook.get_credentials()

        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_bucket_uri = f"s3://{self.s3_bucket}/{rendered_key}"
    
        formatted_sql = f""" 
                COPY {self.table}
                FROM '{s3_bucket_uri}/'
                ACCESS_KEY_ID '{aws_credentials.access_key}'
                SECRET_ACCESS_KEY '{aws_credentials.secret_key}'
                FORMAT AS {self.data_format}
            """

        self.log.info(f"Copying {self.table} data from s3 to redshift")
        redshift_conn.run(formatted_sql)
        return 'Done'
