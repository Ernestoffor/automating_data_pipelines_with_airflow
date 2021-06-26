from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_table_insert = """
                       INSERT INTO {} {}
                            """
    truncate_table_sql = """
                        TRUNCATE TABLE {}
                        """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
               redshift_conn_id="",
                 load_table_query="",
                 table="",
                 operation="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
    
        self. redshift_conn_id=redshift_conn_id
        self.load_table_query=load_table_query
        self.table=table
        self.operation=operation
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Commence Loading Dimension Table {self.table} from staging and song tables in {self.operation} operation')
        if self.operation == "append":
            redshift_hook.run(LoadDimensionOperator.load_table_insert.format(self.table, self.load_table_query))
            self.log.info("Successfully loaded {self.table} dimension_table using customized LoadDimensionOperator.")
        elif self.operation =="truncate":
            redshift_hook.run(LoadDimensionOperator.truncate_table_sql.format(self.table))
            redshift_hook.run(LoadDimensionOperator.load_table_insert.format(self.table, self.load_table_query))
            self.log.info("Successfully loaded {self.table} dimension_table using customized LoadDimensionOperator.")
        else:
            raise ValueError("The {} table failed to load ".format(self.table))