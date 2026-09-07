import operator as op

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

COMPARISONS = {
    '==': op.eq,
    '!=': op.ne,
    '>': op.gt,
    '>=': op.ge,
    '<': op.lt,
    '<=': op.le,
}


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id="redshift",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check['check_sql']
            expected_result = check['expected_result']
            comparison = COMPARISONS[check.get('comparison', '==')]

            self.log.info(f"Running data quality check: {sql}")
            records = redshift.get_records(sql)

            if not records or not records[0]:
                raise ValueError(f"Data quality check failed. Query returned no results: {sql}")

            actual_result = records[0][0]
            if not comparison(actual_result, expected_result):
                raise ValueError(
                    f"Data quality check failed. '{sql}' returned {actual_result}, "
                    f"expected {check.get('comparison', '==')} {expected_result}"
                )

            self.log.info(f"Data quality check passed: {sql}")
