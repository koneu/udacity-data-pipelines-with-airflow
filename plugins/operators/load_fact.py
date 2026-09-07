from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Fact tables are append-only: no delete/truncate before loading.
        self.log.info(f"Loading fact table {self.table}")
        redshift.run(f"INSERT INTO {self.table}\n{self.sql_query}")
        self.log.info(f"Successfully loaded fact table {self.table}")
