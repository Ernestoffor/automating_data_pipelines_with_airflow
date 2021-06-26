from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    load_table_insert = """
                 INSERT INTO {} {}
                            """
    truncate_table_sql = """
                        TRUNCATE TABLE {}
                        """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_table_query="",
                 table="",
                 operation="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self. redshift_conn_id=redshift_conn_id
        self.load_table_query=load_table_query
        self.table=table
        self.operation=operation
        

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Commence Loading Fact Table {self.table} from staging and song tables in {self.operation} operation')
        if self.operation == "append":
            redshift_hook.run(LoadFactOperator.load_table_insert.format(self.table, self.load_table_query))
      
        elif self.operation =="truncate":
            redshift_hook.run(LoadFactOperator.truncate_table_sql.format(self.table))
            redshift_hook.run(LoadFactOperator.load_table_insert.format(self.table, self.load_table_query))
        self.log.info("Successfully loaded {self.table} table using customized LoadFactOperator ")