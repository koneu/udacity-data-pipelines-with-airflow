from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_query="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading dimension table {self.table}")
        redshift.run(f"INSERT INTO {self.table}\n{self.sql_query}")
        self.log.info(f"Successfully loaded dimension table {self.table}")
