import psycopg2
from io import StringIO


def execute(dsn, query):
    con = psycopg2.connect(dsn)
    with con:
        cur = con.cursor()
        cur.execute(query)


def executemany(dsn, query, vars_list):
    con = psycopg2.connect(dsn)
    with con:
        cur = con.cursor()
        cur.executemany(query, vars_list)


def fetchall(dsn, query):
    """ return result rows and header """
    con = psycopg2.connect(dsn)
    with con:
        cur = con.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
    return rows, cols


def copy_from(dsn, path_or_buf, table, sep=',', null='', columns=None):
    con = psycopg2.connect(dsn)
    with con:
        cur = con.cursor()
        cur.copy_from(path_or_buf, table, sep=sep, null=null, columns=columns)
        return cur.rowcount


def copy_from_dataframe(dsn, df, table, sep=',', null='', columns=None):
    """ copy data from pandas.DataFrame object to postgresql """
    sio = StringIO()
    df.to_csv(sio, index=False, header=False, sep=sep)
    sio.seek(0)
    try:
        return copy_from(dsn, sio, table,
            sep=sep, null=null, columns=columns)
    finally:
        sio.close()
