from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import timedelta
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 task_id,
                 sql,
                 postgres_conn_id,
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(task_id=task_id,*args, **kwargs)
        self.task_id=task_id
        self.sql=sql
        self.postgres_conn_id=postgres_conn_id
        

    def execute(self, context):
        # Connect to Postgres
        postgres = PostgresOperator(task_id=self.task_id, sql=self.sql, postgres_conn_id=self.postgres_conn_id, execution_timeout=timedelta(seconds=1200))
        
        # Truncate table and Insert data
        self.log.info("Creating dimension and fact tables: \
            \ni94country\ni94port\ni94mode\ni94address\ni94visa \
            \nairport_codes\nglobal_temperature\ndemographics\nimmigration")
        postgres.execute(self.sql)
        self.log.info("===============================================================================")
        self.log.info("                 Dimension & Fact tables created successfully!                 ")
        self.log.info("===============================================================================")
        