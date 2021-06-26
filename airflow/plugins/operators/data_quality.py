from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        self.log.info('Start implementation of DataQualityOperator ')
        redshift = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            record = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            num_records = record[0][0]
            if(num_records<1):
                raise ValueError(f"Qaulity check failed for {table} table")
            logging.info(f"The {table} table passed the quality test with {num_records}records")