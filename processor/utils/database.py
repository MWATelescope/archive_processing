import psycopg2
from psycopg2.extras import RealDictCursor


def run_sql_get_one_row(database_pool, sql: str, args):
    conn = None
    cur = None

    try:
        conn = database_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if args is None:
            cur.execute(sql)
        else:
            cur.execute(sql, args)

        record = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
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
        conn = database_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if args is None:
            cur.execute(sql)
        else:
            cur.execute(sql, args)

        records = cur.fetchall()

    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if cur:
            cur.close()
        if conn:
            database_pool.putconn(conn)

    return records