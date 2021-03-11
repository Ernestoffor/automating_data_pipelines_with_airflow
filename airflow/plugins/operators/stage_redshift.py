from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto
from boto import ec2



class StageToRedshiftOperator(BaseOperator):
    ec2 = boto.ec2.connect_to_region('us-west-2', profile_name='default')

    access_key = ec2.gs_access_key_id

    secret_key = ec2.gs_secret_access_key

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
                 staging_table = "",
                 s3_bucket ="",
                 s3_region = "us-west-2",
                 s3_key = "",
                 json_path="auto",            
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.staging_table =staging_table
        self.s3_bucket =s3_bucket
        self.s3_region = s3_region
        self.s3_key = s3_key
        self.json_path=json_path
        
    def execute(self, context):
        self.log.info('Start implementation of StageToRedshiftOperator')
        ec2 = boto.ec2.connect_to_region('us-west-2', profile_name='default')
        access_key = ec2.gs_access_key_id
        secret_key = ec2.gs_secret_access_key
        # aws_hook = AwsHook(self.aws_credentials_id)
        # credentials = aws_hook.get_credentials()
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
            access_key,
            secret_key,
            self.json_path
          )
        redshift.run(execution_sql)
        





