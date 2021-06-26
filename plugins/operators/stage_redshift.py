from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3 
from airflow.models import Variable
import os
from boto3 import ec2
from boto3 import Session

#session = Session('us-west-2', profile_name='default')
#credentials = session.get_credentials()
# Credentials are refreshable, so accessing your access key / secret key
# separately can lead to a race condition. Use this to get an actual matched
# set.
#current_credentials = credentials.get_frozen_credentials()
#access_key = Variable.get('AWS_ACCESS_KEY_ID')
#secret_key = Variable.get('AWS_SECRET_ACCESS_KEY')


#access_key= current_credentials.access_key
#secret_key= current_credentials.secret_key

boto_client = boto3.setup_default_session(region_name='us-west-2')
#ec2 = boto3.client('iam')

# session = boto3.Session(
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRET_KEY)
#ec2 = boto3.ec2.connect_to_region('us-west-2', profile_name='default')



class StageToRedshiftOperator(BaseOperator):
     copy_table_sql = """
             COPY {} 
             FROM '{}'
             aws_access_key_id  '{}'
             aws_secret_access_key '{}'
             FORMAT AS JSON '{}'
                  """
    
     ui_color = '#358140'
     template_fields = ("s3_key",)
       
     @apply_defaults
     def __init__(self,
                 redshift_conn_id="",
                 staging_table = "",
                 s3_bucket ="",
                 aws_credentials_id="",
                 s3_key = "",
                 json_path="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id
        self.staging_table =staging_table
        self.s3_bucket =s3_bucket
        self.aws_credentials_id= aws_credentials_id
        self.s3_key = s3_key
        self.json_path=json_path
        
                
        
     def execute(self, context):
         self.log.info('Start implementation of StageToRedshiftOperator')
         s3 = S3Hook(aws_conn_id=self.aws_credentials_id) 
        #aws_hook = AwsHook(self.aws_credentials_id)
         # aws_hook = AwsHook(self.aws_credentials_id)
         # credentials = aws_hook.get_credentials()
         redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) 
        
         self.log.info("Preparing destination Redshift table")
         redshift.run("DELETE FROM {}".format(self.staging_table))
        
         self.log.info("Getting data from S3 to Redshift")
         rendered_s3_key = self.s3_key.format(**context)
         s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_s3_key)
         execution_sql = StageToRedshiftOperator.copy_table_sql.format(
            self.staging_table,
            s3_path,
            s3.get_credentials().access_key,
            s3.get_credentials().secret_key,
            self.json_path
      
            )
         redshift.run(execution_sql)
        



