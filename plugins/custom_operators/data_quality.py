from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from datetime import timedelta
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 task_id,
                 postgres_conn_id,
                 dq_checks: list[dict],
                 *args, **kwargs):        
        super(DataQualityOperator, self).__init__(task_id=task_id,*args, **kwargs)
        self.task_id=task_id
        self.dq_checks = dq_checks
        self.postgres_conn_id=postgres_conn_id
        
    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, execution_timeout=timedelta(seconds=300))
        
        self.log.info("Starting with Data Quality checks\n========================================================\n")
        error_count = 0
        failing_tests = []

        self.log.info(type(self.dq_checks))

        for check in self.dq_checks:
            sql = check["sql_check"]
            exp_result = check["expected_result"]
            self.log.info(f'Running SQL: {sql} =====> Expected Result: {exp_result}')
            conn=postgres_hook.get_conn()
            cur=conn.cursor()
            cur.execute(sql)
            records=cur.fetchall()[0][0]
            self.log.info(f'Records retrieved: {records}')

            if int(exp_result) != int(records):
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info('Tests failed:')
            self.log.info(f'{failing_tests}\n========================================================\n')
            raise ValueError('Data quality check failed')
        else:
            self.log.info('Data Quality checks passed!')