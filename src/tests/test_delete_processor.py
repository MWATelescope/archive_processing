import os
import sys
import argparse

import boto3
import pytest
import psycopg

from psycopg import Connection
from psycopg.rows import dict_row
from pytest_postgresql import factories
from moto import mock_s3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
file_path = os.path.abspath(os.path.dirname(__file__))

from processor import ProcessorFactory, DeleteProcessor
from main import parse_arguments


def load_database(**kwargs):
    db_connection: Connection = psycopg.connect(**kwargs)
    
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


@pytest.fixture
def delete_processor(postgresql):
    args_list = ['delete']
    args = parse_arguments(args=args_list)

    processor_factory = ProcessorFactory(args, connection=postgresql)
    return processor_factory.get_processor()


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


def test_parse_args():
    args_list = ['delete', '--verbose', '--dry_run', '--cfg=test']

    args = parse_arguments(args=args_list)

    assert(args.subcommand == 'delete')
    assert(args.verbose)
    assert(args.dry_run)
    assert(args.cfg == 'test')


def test_processor_factory(postgresql):
    args_list = ['delete', '--verbose', '--dry_run']

    args = parse_arguments(args=args_list)

    processor_factory = ProcessorFactory(args, connection=postgresql)
    processor = processor_factory.get_processor()

    assert(isinstance(processor, DeleteProcessor))
    assert(processor.verbose)
    assert(processor.dry_run)


def test_generate_data_structures(delete_processor):
    files, delete_requests = delete_processor._generate_data_structures()

    assert(files == {
        2: {
            'mwa01fs': {
                '/bucket1/13_2.fits', 
                '/bucket1/11_6.fits', 
                '/bucket1/12_4.fits', 
                '/bucket1/11_1.fits', 
                '/bucket1/21_2.fits', 
                '/bucket2/22_1.fits', 
                '/bucket1/21_6.fits', 
                '/bucket1/11_5.fits', 
                '/bucket4/22_5.fits'
            }, 
            'mwa02fs': {
                '/bucket2/11_2.fits', 
                '/bucket2/12_5.fits', 
                '/bucket4/22_6.fits', 
                '/bucket2/12_1.fits', 
                '/bucket2/13_3.fits', 
                '/bucket2/21_3.fits', 
                '/bucket3/22_2.fits'
            }, 
            'mwa03fs': {
                '/bucket3/21_4.fits', 
                '/bucket3/12_6.fits', 
                '/bucket3/13_4.fits', 
                '/bucket3/13_5.fits', 
                '/bucket4/22_3.fits', 
                '/bucket3/13_6.fits', 
                '/bucket3/11_3.fits', 
                '/bucket3/12_2.fits'
            }, 
            'mwa04fs': {
                '/bucket4/13_1.fits', 
                '/bucket4/11_4.fits', 
                '/bucket4/22_4.fits', 
                '/bucket4/21_1.fits', 
                '/bucket4/12_3.fits', 
                '/bucket4/21_5.fits'
            }
        }, 
        3: {
            'mwaingest-23': {
                '23_5.fits', 
                '23_2.fits', 
                '23_3.fits', 
                '23_6.fits', 
                '23_1.fits', 
                '23_4.fits'
            }, 
            'mwaingest-31': {
                '31_5.fits', 
                '31_4.fits', 
                '31_6.fits', 
                '31_1.fits', 
                '31_3.fits', 
                '31_2.fits'
            }, 
            'mwaingest-32': {
                '32_2.fits', 
                '32_1.fits', 
                '32_3.fits', 
                '32_6.fits', 
                '32_4.fits', 
                '32_5.fits'
            }, 
            'mwaingest-33': {
                '33_6.fits', 
                '33_1.fits', 
                '33_3.fits', 
                '33_2.fits', 
                '33_4.fits', 
                '33_5.fits'
            }
        }
    })

    assert(delete_requests == {
        1: [11, 12, 13], 
        2: [21, 22, 23], 
        3: [31, 32, 33]
    })


def test_get_delete_requests(delete_processor):
    assert(delete_processor.repository.get_delete_requests() == [1,2,3])


def test_get_obs_ids_for_delete_request(delete_processor):
    assert(delete_processor.repository.get_obs_ids_for_delete_request(1) == [11,12,13])
    assert(delete_processor.repository.get_obs_ids_for_delete_request(2) == [21,22,23])
    assert(delete_processor.repository.get_obs_ids_for_delete_request(3) == [31,32,33])


def get_obs_data_files_filenames_except_ppds(delete_processor):
    assert(delete_processor.repository.get_obs_data_files_filenames_except_ppds(11) ==
        [
            {
                'location': 2,
                'bucket': 'mwa01fs',
                'key': '/bucket1/11_1.fits',
                'filename': '11_1.fits'
            },
            {
                'location': 2,
                'bucket': 'mwa02fs',
                'key': '/bucket2/11_2.fits',
                'filename': '11_2.fits'
            },
            {
                'location': 2,
                'bucket': 'mwa03fs',
                'key': '/bucket3/11_3.fits',
                'filename': '11_3.fits'
            },
            {
                'location': 2,
                'bucket': 'mwa04fs',
                'key': '/bucket4/11_4.fits',
                'filename': '11_4.fits'
            },
            {
                'location': 2,
                'bucket': 'mwa01fs',
                'key': '/bucket1/11_5.fits',
                'filename': '11_5.fits'
            },
            {
                'location': 2,
                'bucket': 'mwa01fs',
                'key': '/bucket1/11_6.fits',
                'filename': '11_6.fits'
            }
        ]
    )

    assert(delete_processor.repository.get_obs_data_files_filenames_except_ppds(31) ==
        [
            {
                'location': 3,
                'bucket': 'mwaingest-31',
                'key': '31_1.fits',
                'filename': '31_1.fits'
            },
            {
                'location': 3,
                'bucket': 'mwaingest-31',
                'key': '31_2.fits',
                'filename': '31_2.fits'
            },
            {
                'location': 3,
                'bucket': 'mwaingest-31',
                'key': '31_3.fits',
                'filename': '31_3.fits'
            },
            {
                'location': 3,
                'bucket': 'mwaingest-31',
                'key': '31_4.fits',
                'filename': '31_4.fits'
            },
            {
                'location': 3,
                'bucket': 'mwaingest-31',
                'key': '31_5.fits',
                'filename': '31_5.fits'
            },
            {
                'location': 3,
                'bucket': 'mwaingest-31',
                'key': '31_6.fits',
                'filename': '31_6.fits'
            }
        ]
    )


def failing_mock_delete_function(*args):
    raise Exception


def passing_mock_delete_function(*args):
    return None


def filter_undeleted_files(obs_files):
    return [file for file in obs_files if file['deleted'] != True]


def test_delete_files(delete_processor):
    obs_files = [
        {
            'location': 2, 
            'bucket': 'mwa01fs', 
            'key': '/bucket1/11_1.fits', 
            'filename': '11_1.fits', 
            'deleted': False
        }, 
        {
            'location': 2, 
            'bucket': 'mwa02fs', 
            'key': '/bucket2/11_2.fits', 
            'filename': '11_2.fits', 
            'deleted': False
        }, 
        {
            'location': 2, 
            'bucket': 'mwa03fs', 
            'key': '/bucket3/11_3.fits', 
            'filename': '11_3.fits', 
            'deleted': False
        }, 
        {
            'location': 2, 
            'bucket': 'mwa04fs', 
            'key': '/bucket4/11_4.fits', 
            'filename': '11_4.fits', 
            'deleted': False
        }, 
        {
            'location': 2, 
            'bucket': 'mwa01fs', 
            'key': '/bucket1/11_5.fits', 
            'filename': '11_5.fits', 
            'deleted': False
        }, 
        {
            'location': 2, 
            'bucket': 'mwa01fs', 
            'key': '/bucket1/11_6.fits', 
            'filename': '11_6.fits', 
            'deleted': False
        }
    ]

    assert(
        filter_undeleted_files(
            delete_processor.repository.get_obs_data_files_filenames_except_ppds(11)
        ) == obs_files
    )

    with pytest.raises(Exception):
        delete_processor.repository.update_files_to_deleted(failing_mock_delete_function, None, obs_files)

    assert(
        filter_undeleted_files(
            delete_processor.repository.get_obs_data_files_filenames_except_ppds(11)
        ) == obs_files
    )

    delete_processor.repository.update_files_to_deleted(passing_mock_delete_function, None, [file['filename'] for file in obs_files])

    assert(
        filter_undeleted_files(
            delete_processor.repository.get_obs_data_files_filenames_except_ppds(11)
        ) == []
    )

    delete_processor.repository.set_obs_id_to_deleted(11)

    sql = """SELECT deleted, deleted_timestamp
            FROM mwa_setting
            WHERE starttime = %s"""

    params = (11,)

    observation = delete_processor.repository.run_sql_get_one_row(sql, params)

    assert(observation['deleted'] == True)
    assert(observation['deleted_timestamp'] != None)

    delete_processor.repository.set_delete_request_to_actioned(1)

    sql = """SELECT actioned_datetime
            FROM deletion_requests
            WHERE id = %s"""

    params = (1,)

    deletion_request = delete_processor.repository.run_sql_get_one_row(sql, params)

    assert(deletion_request['actioned_datetime'] != None)


@mock_s3
def test_run(delete_processor, aws_credentials):
    #From the top now
    setup_buckets(delete_processor.repository.conn)
    delete_processor.run()

    sql = """SELECT *
            FROM deletion_requests
            WHERE actioned_datetime IS NULL"""

    unactioned_delete_requests = delete_processor.repository.run_sql_get_many_rows(sql, None)
    assert(len(unactioned_delete_requests) == 0)

    sql = """SELECT *
            FROM mwa_setting
            WHERE NOT deleted
            OR deleted_timestamp IS NULL"""

    undeleted_obs_ids = delete_processor.repository.run_sql_get_many_rows(sql, None)
    assert(len(undeleted_obs_ids) == 0)

    sql = """SELECT *
            FROM data_files
            WHERE NOT deleted
            OR deleted_timestamp IS NULL"""

    undeleted_files = delete_processor.repository.run_sql_get_many_rows(sql, None)
    assert(len(undeleted_files) == 0)