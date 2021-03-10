from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email': ['ernest@gmail.com.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
       task_id='create_tables',
       dag=dag,
       postgres_conn_id="redshift",
       sql= "create_tables.sql"
       )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id ="aws_credentials",
    staging_table = "staging_events",
    s3_bucket ="udacity-dend",
    s3_key = "log_data/2018/11/",
    json_path = 'log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id ="aws_credentials",
    staging_table = "staging_songs",
    s3_bucket='udacity-dend',
    s3_key='song_data/*/*/*/'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id= "redshift",
    load_table_query= SqlQueries.songplay_table_insert,
    table="songplays",
    operation="append"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id= "redshift",
    load_table_query= SqlQueries.user_table_insert,
    table="users",
    operation="append"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id= "redshift",
    load_table_query= SqlQueries.song_table_insert,
    table="songs",
    operation="append"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id= "redshift",
    load_table_query= SqlQueries.artist_table_insert,
    table="artists",
    operation="append"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id= "redshift",
    load_table_query= SqlQueries.time_table_insert,
    table="time",
    operation="append"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_artist_dimension_table

load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
