from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        REGION '{}'
        JSON '{}'
    """

    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='sts')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        # Temporary (STS) credentials require a session token alongside the key pair.
        session_token_clause = f"SESSION_TOKEN '{credentials.token}'" if credentials.token else ""

        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            session_token_clause,
            self.region,
            self.json_path,
        )
        redshift.run(formatted_sql)
        self.log.info(f"Successfully copied data to {self.table}")
