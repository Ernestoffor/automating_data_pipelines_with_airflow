from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    copy_table_sql = """
                 COPY {} 
                 FROM '{}'
                 REGION '{}'
                 aws_access_key_id  '{}'
                 aws_secret_access_key '{}'
                 FORMAT AS JSON '{}'
                      """
    
    delete_table_sql = """
                        DELETE FROM {} 
                        """

    ui_color = '#358140'

    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id ="",
                 staging_table = "",
                 s3_bucket ="",
                 s3_region = "us-west-2",
                 s3_key = "",
                 json_path="auto",


                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.aws_credentials_id =aws_credentials_id
        self.staging_table =staging_table
        self.s3_bucket =s3_bucket
        self.s3_region = s3_region
        self.s3_key = s3_key
        self.json_path=json_path
        
    def execute(self, context):
        self.log.info('Start implementation of StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
        self.log.info("Preparing destination Redshift table")
        redshift.run( StageToRedshiftOperator.delete_table_sql.format(self.staging_table))
        
        self.log.info("Getting data from S3 to Redshift")
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_s3_key)
        execution_sql = StageToRedshiftOperator.copy_table_sql.format(
            self.staging_table,
            s3_path,
            self.s3_region,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
            
          )
        redshift.run(execution_sql)
        





