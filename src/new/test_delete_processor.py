import pytest
import argparse

from processor import ProcessorFactory, DeleteProcessor
from main import parse_arguments

from pytest_postgresql import factories


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


# @pytest.fixture()
# def database(postgresql):
#     with open("test.sql") as f:
#         setup_sql = f.read()

#     with postgresql.cursor() as cursor:
#         cursor.execute(setup_sql)
#         postgresql.commit()

#     print(postgresql)

#     yield postgresql


# def test_example_postgres(database):
#     print(database)

#     assert(True)

postgresql_my_proc = factories.postgresql_proc(
    port=5432, unixsocketdir='/var/run'
)
    
postgresql_my = factories.postgresql('postgresql_my_proc')

def test_example_postgres(postgresql_my):

    """Check main postgresql fixture."""
    cur = postgresql_my.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    postgresql_my.commit()
    cur.close()
