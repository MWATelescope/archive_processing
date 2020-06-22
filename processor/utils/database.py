import psycopg2
from psycopg2.extras import RealDictCursor


def run_sql_get_one_row(database_pool, sql: str, args):
    conn = None
    cur = None

    try:
        if database_pool:
            conn = database_pool.getconn()

            cur = conn.cursor(cursor_factory=RealDictCursor)

            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)

            record = cur.fetchone()
            conn.commit()
        else:
            raise Exception("database pool is not initialised")

    except Exception as error:
        if conn:
            conn.rollback()
        raise error

    finally:
        if cur:
            cur.close()
        if conn:
            database_pool.putconn(conn)

    return record


def run_sql_get_many_rows(database_pool, sql: str, args) -> list:
    conn = None
    cur = None

    try:
        if database_pool:
            conn = database_pool.getconn()

            cur = conn.cursor(cursor_factory=RealDictCursor)

            if args is None:
                cur.execute(sql)
            else:
                cur.execute(sql, args)

            records = cur.fetchall()

            conn.commit()

        else:
            raise Exception("database pool is not initialised")

    except Exception as error:
        if conn:
            conn.rollback()
        raise error

    finally:
        if cur:
            cur.close()
        if conn:
            database_pool.putconn(conn)
    return records
