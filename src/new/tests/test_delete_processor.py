import os
import sys
import argparse

import boto3
import pytest
import psycopg

from psycopg.rows import dict_row
from pytest_postgresql import factories
from moto import mock_s3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
file_path = os.path.abspath(os.path.dirname(__file__))

from processor import ProcessorFactory, DeleteProcessor
from main import parse_arguments

os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://ingest.pawsey.org.au,https://vss-1.pawsey.org.au"


def load_database(**kwargs):
    db_connection: connection = psycopg.connect(**kwargs)
    
    with db_connection.cursor() as cur:
        cur.execute(open(os.path.join(file_path, "test_schema.sql"), "r").read())
        db_connection.commit()


postgresql_noproc = factories.postgresql_noproc(
    load=[load_database],
)

postgresql = factories.postgresql(
    "postgresql_noproc",
)

@pytest.fixture(scope='function')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://ingest.pawsey.org.au,https://vss-1.pawsey.org.au"


def test_parse_args():
    args_list = ['delete', '--verbose', '--dry_run', '--cfg=test']

    args = parse_arguments(args=args_list)

    assert(args.subcommand == 'delete')
    assert(args.verbose)
    assert(args.dry_run)
    assert(args.cfg == 'test')


def test_processor_factory():
    args_list = ['delete', '--verbose', '--dry_run']

    args = parse_arguments(args=args_list)

    processor_factory = ProcessorFactory(args)
    processor = processor_factory.get_processor()

    assert(isinstance(processor, DeleteProcessor))
    assert(processor.verbose)
    assert(processor.dry_run)


#TODO put this into a setup function in a class
def setup_buckets(postgresql):
    with postgresql.cursor(row_factory=dict_row) as cur:
        sql = """
            select distinct bucket from data_files
        """

        cur.execute(sql)
        records = cur.fetchall()

    for record in records:
        s3 = boto3.resource(
            's3', 
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            endpoint_url='https://ingest.pawsey.org.au'
        )
        s3.create_bucket(Bucket=record['bucket'])


@mock_s3
def test_processor_with_connection(postgresql):
    args_list = ['delete', '--verbose', '--dry_run']
    args = parse_arguments(args=args_list)

    processor_factory = ProcessorFactory(args, connection=postgresql)
    processor = processor_factory.get_processor()

    setup_buckets(postgresql)

    try:
        processor.run()
    except Exception as e:

        raise


class TestBase:
    def setup_class(self):
        pass
