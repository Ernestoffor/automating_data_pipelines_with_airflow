from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    test_dict = [{"test_sql": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", "desired_result":0},
                {"test_sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "desired_result":0},
                {"test_sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", "desired_result":0},
                {"test_sql": "SELECT COUNT(*) FROM time WHERE start_time IS NULL", "desired_result":0},
                {"test_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "desired_result":0}
    ]

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=tables

    def execute(self, context):
        self.log.info('Start implementation of Data Quality using \
            the fact and dimensional tables ')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            record = redshift.get_records(f"SELECT COUNT(*) FROM\
                 {table}")
            num_records = record[0][0]
            if(num_records<1):
                raise ValueError("Qaulity check failed for\
                     {} table showing it is empty".format(table))
            logging.info("The {} table passed the quality\
                 test with {}records".formats(table, num_records))

        logging.info("Checking Null Values in tables")
        tests = DataQualityOperator.test_dict
        number_of_null_tables= 0
        failed_tests = []
        for query in DataQualityOperator.test_dict:
            actual_result = query.get("test_sql")
            expected_result = query.get("desired_result")
            result = redshift.get_records(actual_result)[0][0]

            if result > expected_result:
                number_of_null_tables +=1
                failed_tests.append(query)
        if number_of_null_tables > 0:
            raise ValueError("NULL TEST FAILURE")
            self.log.info("The {} tests out of {} tests have NULL\
                 VALUES ".format(number_of_null_tables, len(tests)))
            self.log.info(failed_tests)
        else:
            self.log.info("All the {} tests PASSED THE NULL TEST "\
                .format(len(tests)))                


