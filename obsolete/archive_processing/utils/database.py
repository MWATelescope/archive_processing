import psycopg_pool
from psycopg.rows import dict_row


def run_sql_get_one_row(
    database_pool: psycopg_pool.ConnectionPool, sql: str, args
):
    cur = None
    record = None

    try:
        if database_pool:
            database_pool.check()

            with database_pool.connection() as conn:
                cur = conn.cursor(row_factory=dict_row)

                if args is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, args)

                record = cur.fetchone()
        else:
            raise Exception("database pool is not initialised")

    except Exception as error:
        raise error

    finally:
        if cur:
            cur.close()

    return record


def run_sql_get_many_rows(
    database_pool: psycopg_pool.ConnectionPool, sql: str, args
) -> list:
    records = []
    cur = None

    try:
        if database_pool:
            database_pool.check()

            with database_pool.connection() as conn:
                cur = conn.cursor(row_factory=dict_row)

                if args is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, args)

                records = cur.fetchall()
        else:
            raise Exception("database pool is not initialised")

    except Exception as error:
        raise error

    finally:
        if cur:
            cur.close()
    return records
