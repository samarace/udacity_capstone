from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 task_id,
                 sql,
                 postgres_conn_id,
                 table,
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(task_id=task_id,*args, **kwargs)
        self.task_id=task_id
        self.sql=sql
        self.postgres_conn_id=postgres_conn_id
        self.table=table
        

    def execute(self, context):
        # Connect to Postgres
        postgres = PostgresOperator(task_id=self.task_id, sql=self.sql, postgres_conn_id=self.postgres_conn_id, execution_timeout=timedelta(seconds=3600))
        
        # Truncate table and Insert data
        self.log.info("Truncate table {} \n==========================================================================\n".format(self.table))
        self.log.info("Load table {}".format(self.table))
        postgres.execute(self.sql)
        self.log.info("Values successfully inserted in {} table \n==========================================================================\n".format(self.table))

