"""To run tests use:
pytest --postgresql-host=127.0.0.1 --postgresql-password=postgres

It assumes you have a local Postgresql database server listening on port 5432
with a default super user of 'postgres' with password: 'postgres'

We use moto to mock S3 calls so no worries about the delete processor
really doing any deletes!  BUT as a precaution set the config file values
for the database and the S3 endpoints and profiles to be dummy ones...
JUST IN CASE!"""

import os
import sys
import boto3
import psycopg
import pytest
from moto import mock_aws
from psycopg import Connection
from psycopg.rows import dict_row
from pytest_postgresql import factories
from main import parse_arguments, get_dsn, read_config
from delete_processor import DeleteProcessor
from delete_repository import DeleteRepository

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
file_path = os.path.abspath(os.path.dirname(__file__))


def load_database(**kwargs):
    db_connection: Connection = psycopg.connect(**kwargs)

    with db_connection.cursor() as cur:
        cur.execute(open(os.path.join(file_path, "test_schema.sql"), "r").read())
        db_connection.commit()


postgresql_noproc = factories.postgresql_noproc(
    load=[load_database],
)


postgresql = factories.postgresql("postgresql_noproc")


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://test-acacia.pawsey.org.au,https://test-banksia.pawsey.org.au:9000"


@pytest.fixture
def delete_processor(postgresql):
    args_list = ["delete", "--cfg=cfg/test_delete_processor.cfg"]
    args = parse_arguments(args=args_list)
    config = read_config(args.cfg)
    dsn = get_dsn(config)

    repository = DeleteRepository(
        dsn=dsn,
        connection=postgresql,
        webservices_url=config.get("webservices", "url"),
        dry_run=args.dry_run,
    )
    return DeleteProcessor(repository=repository, dry_run=args.dry_run, config=config)


@mock_aws
def setup_buckets(postgresql):
    # The @mock_s3 attribute automagically converts all real s3
    # calls to a mock s3 instance which lives and dies with the test session
    with postgresql.cursor(row_factory=dict_row) as cur:
        sql = """
            select distinct bucket from data_files
        """

        cur.execute(sql)
        records = cur.fetchall()

    for record in records:
        s3 = boto3.resource(
            "s3",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            endpoint_url="https://test-acacia.pawsey.org.au",
        )
        s3.create_bucket(Bucket=record["bucket"])


def test_parse_args():
    args_list = ["delete", "--verbose", "--dry_run", "--cfg=test"]

    args = parse_arguments(args=args_list)

    assert args.subcommand == "delete"
    assert args.verbose
    assert args.dry_run
    assert args.cfg == "test"


def test_generate_data_structures(delete_processor):
    files, delete_requests = delete_processor._generate_data_structures()

    assert files == {
        2: {
            "mwa01fs": {
                "/bucket1/9000000013_2.fits",
                "/bucket1/9000000011_6.fits",
                "/bucket1/9000000012_4.fits",
                "/bucket1/9000000011_1.fits",
                "/bucket1/9000000021_2.fits",
                "/bucket2/9000000022_1.fits",
                "/bucket1/9000000021_6.fits",
                "/bucket1/9000000011_5.fits",
                "/bucket4/9000000022_5.fits",
            },
            "mwa02fs": {
                "/bucket2/9000000011_2.fits",
                "/bucket2/9000000012_5.fits",
                "/bucket4/9000000022_6.fits",
                "/bucket2/9000000012_1.fits",
                "/bucket2/9000000013_3.fits",
                "/bucket2/9000000021_3.fits",
                "/bucket3/9000000022_2.fits",
            },
            "mwa03fs": {
                "/bucket3/9000000021_4.fits",
                "/bucket3/9000000012_6.fits",
                "/bucket3/9000000013_4.fits",
                "/bucket3/9000000013_5.fits",
                "/bucket4/9000000022_3.fits",
                "/bucket3/9000000013_6.fits",
                "/bucket3/9000000011_3.fits",
                "/bucket3/9000000012_2.fits",
            },
            "mwa04fs": {
                "/bucket4/9000000013_1.fits",
                "/bucket4/9000000011_4.fits",
                "/bucket4/9000000022_4.fits",
                "/bucket4/9000000021_1.fits",
                "/bucket4/9000000012_3.fits",
                "/bucket4/9000000021_5.fits",
            },
        },
        3: {
            "mwaingest-23": {
                "9000000023_5.fits",
                "9000000023_2.fits",
                "9000000023_3.fits",
                "9000000023_6.fits",
                "9000000023_1.fits",
                "9000000023_4.fits",
            },
            "mwaingest-31": {
                "9000000031_5.fits",
                "9000000031_4.fits",
                "9000000031_6.fits",
                "9000000031_1.fits",
                "9000000031_3.fits",
                "9000000031_2.fits",
            },
            "mwaingest-32": {
                "9000000032_2.fits",
                "9000000032_1.fits",
                "9000000032_3.fits",
                "9000000032_6.fits",
                "9000000032_4.fits",
                "9000000032_5.fits",
            },
            "mwaingest-33": {
                "9000000033_6.fits",
                "9000000033_1.fits",
                "9000000033_3.fits",
                "9000000033_2.fits",
                "9000000033_4.fits",
                "9000000033_5.fits",
            },
            "mwaingest-41": {
                "9000000041_3.sub",
                "9000000041_4.sub",
            },
        },
    }

    assert delete_requests == {
        1: [9000000011, 9000000012, 9000000013],
        2: [9000000021, 9000000022, 9000000023],
        3: [9000000031, 9000000032, 9000000033],
        4: [9000000041],
    }


def test_get_delete_requests(delete_processor):
    assert delete_processor.repository.get_delete_requests() == [1, 2, 3, 4]


def test_get_filetype_id_for_delete_request(delete_processor):
    assert delete_processor.repository.get_filetype_id_for_delete_request(1) is None
    assert delete_processor.repository.get_filetype_id_for_delete_request(4) == 17


def test_get_obs_ids_for_delete_request(delete_processor):
    assert delete_processor.repository.get_obs_ids_for_delete_request(1) == [9000000011, 9000000012, 9000000013]
    assert delete_processor.repository.get_obs_ids_for_delete_request(2) == [9000000021, 9000000022, 9000000023]
    assert delete_processor.repository.get_obs_ids_for_delete_request(3) == [9000000031, 9000000032, 9000000033]
    assert delete_processor.repository.get_obs_ids_for_delete_request(4) == [9000000041, 9000000042]


def get_obs_data_files_filenames_except_ppds(delete_processor):
    assert delete_processor.repository.get_not_deleted_obs_data_files_except_ppds(9000000011) == [
        {
            "location": 2,
            "bucket": "mwa01fs",
            "key": "/bucket1/9000000011_1.fits",
            "filename": "9000000011_1.fits",
        },
        {
            "location": 2,
            "bucket": "mwa02fs",
            "key": "/bucket2/9000000011_2.fits",
            "filename": "9000000011_2.fits",
        },
        {
            "location": 2,
            "bucket": "mwa03fs",
            "key": "/bucket3/9000000011_3.fits",
            "filename": "9000000011_3.fits",
        },
        {
            "location": 2,
            "bucket": "mwa04fs",
            "key": "/bucket4/9000000011_4.fits",
            "filename": "9000000011_4.fits",
        },
        {
            "location": 2,
            "bucket": "mwa01fs",
            "key": "/bucket1/9000000011_5.fits",
            "filename": "9000000011_5.fits",
        },
        {
            "location": 2,
            "bucket": "mwa01fs",
            "key": "/bucket1/9000000011_6.fits",
            "filename": "9000000011_6.fits",
        },
    ]

    assert delete_processor.repository.get_not_deleted_obs_data_files_except_ppds(9000000031) == [
        {
            "location": 3,
            "bucket": "mwaingest-31",
            "key": "9000000031_1.fits",
            "filename": "9000000031_1.fits",
        },
        {
            "location": 3,
            "bucket": "mwaingest-31",
            "key": "9000000031_2.fits",
            "filename": "9000000031_2.fits",
        },
        {
            "location": 3,
            "bucket": "mwaingest-31",
            "key": "9000000031_3.fits",
            "filename": "9000000031_3.fits",
        },
        {
            "location": 3,
            "bucket": "mwaingest-31",
            "key": "9000000031_4.fits",
            "filename": "9000000031_4.fits",
        },
        {
            "location": 3,
            "bucket": "mwaingest-31",
            "key": "9000000031_5.fits",
            "filename": "9000000031_5.fits",
        },
        {
            "location": 3,
            "bucket": "mwaingest-31",
            "key": "9000000031_6.fits",
            "filename": "9000000031_6.fits",
        },
    ]


def failing_mock_delete_function(*args):
    raise Exception


def passing_mock_delete_function(*args):
    response = {
        "Deleted": [
            {"Key": "/bucket1/9000000011_1.fits"},
            {"Key": "/bucket2/9000000011_2.fits"},
            {"Key": "/bucket3/9000000011_3.fits"},
            {"Key": "/bucket4/9000000011_4.fits"},
            {"Key": "/bucket1/9000000011_5.fits"},
            {"Key": "/bucket1/9000000011_6.fits"},
        ]
    }

    deleted_objects = response["Deleted"]
    deleted_keys = [deleted_object["Key"] for deleted_object in deleted_objects]

    # we need to return filenames and a unique list of obsids

    return [
        [os.path.split(key)[-1] for key in deleted_keys],
        [
            9000000011,
        ],
    ]


@mock_aws
def test_delete_files(delete_processor):
    obs_files = [
        {
            "location": 2,
            "bucket": "mwa01fs",
            "key": "/bucket1/9000000011_1.fits",
            "filename": "9000000011_1.fits",
        },
        {
            "location": 2,
            "bucket": "mwa02fs",
            "key": "/bucket2/9000000011_2.fits",
            "filename": "9000000011_2.fits",
        },
        {
            "location": 2,
            "bucket": "mwa03fs",
            "key": "/bucket3/9000000011_3.fits",
            "filename": "9000000011_3.fits",
        },
        {
            "location": 2,
            "bucket": "mwa04fs",
            "key": "/bucket4/9000000011_4.fits",
            "filename": "9000000011_4.fits",
        },
        {
            "location": 2,
            "bucket": "mwa01fs",
            "key": "/bucket1/9000000011_5.fits",
            "filename": "9000000011_5.fits",
        },
        {
            "location": 2,
            "bucket": "mwa01fs",
            "key": "/bucket1/9000000011_6.fits",
            "filename": "9000000011_6.fits",
        },
    ]

    assert delete_processor.repository.get_not_deleted_obs_data_files_except_ppds(9000000011, None) == obs_files

    with pytest.raises(Exception):
        delete_processor.repository.update_files_to_deleted(failing_mock_delete_function, None, obs_files)

    assert delete_processor.repository.get_not_deleted_obs_data_files_except_ppds(9000000011, None) == obs_files

    delete_processor.repository.update_files_to_deleted(
        passing_mock_delete_function,
        None,
        [
            [file["filename"] for file in obs_files],
        ],
    )

    assert delete_processor.repository.get_not_deleted_obs_data_files_except_ppds(9000000011, None) == []

    delete_processor.repository.set_obs_id_to_deleted(9000000011)

    sql = """SELECT deleted, deleted_timestamp
            FROM mwa_setting
            WHERE starttime = %s"""

    params = (9000000011,)

    observation = delete_processor.repository.run_sql_get_one_row(sql, params)

    assert observation["deleted"] is True
    assert observation["deleted_timestamp"] is not None

    delete_processor.repository.set_delete_request_to_actioned(1)

    sql = """SELECT actioned_datetime
            FROM deletion_requests
            WHERE id = %s"""

    params = (1,)

    deletion_request = delete_processor.repository.run_sql_get_one_row(sql, params)

    assert deletion_request["actioned_datetime"] is not None


@mock_aws
def test_run(delete_processor, aws_credentials):
    # From the top now
    setup_buckets(delete_processor.repository.conn)
    delete_processor.run()

    sql = """SELECT id
            FROM deletion_requests
            WHERE actioned_datetime IS NULL"""

    unactioned_delete_requests = delete_processor.repository.run_sql_get_many_rows(sql, None)
    assert len(unactioned_delete_requests) == 0, "unactioned_delete_requests"

    sql = """SELECT starttime
            FROM mwa_setting
            WHERE deleted_timestamp IS NULL"""

    undeleted_obs_ids = delete_processor.repository.run_sql_get_many_rows(sql, None)

    # 9000000041 not deleted because it was in a DR which had filetype=17 (subfiles)
    # 9000000042 not deleted because it was in a DR which had filetype=17 (subfiles)
    assert len(undeleted_obs_ids) == 2, "undeleted_obs_ids"
    assert undeleted_obs_ids[0]["starttime"] == 9000000041
    assert undeleted_obs_ids[1]["starttime"] == 9000000042

    sql = """SELECT filename
            FROM data_files
            WHERE deleted_timestamp IS NULL"""

    undeleted_files = delete_processor.repository.run_sql_get_many_rows(sql, None)

    # '9000000041_1.fits' not deleted because it was in a DR which had filetype=17 (subfiles)
    # '9000000041_2.fits' not deleted because it was in a DR which had filetype=17 (subfiles)
    # '9000000042_1.fits' not deleted because it was in a DR which had filetype=17 (subfiles)
    assert len(undeleted_files) == 3, "undeleted_files"
    assert undeleted_files[0]["filename"] == "9000000041_1.fits"
    assert undeleted_files[1]["filename"] == "9000000041_2.fits"
    assert undeleted_files[2]["filename"] == "9000000042_1.fits"
