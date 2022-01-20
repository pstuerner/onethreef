import re
from onethreef.constants import _init_connection
from onethreef.write import run_query

def drop_all_portfolios(really=False):
    if really:
        conn = _init_connection()
        tables = [t[0] for t in run_query(conn, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")]

        with conn.cursor() as cur:
            for table in tables:
                if re.match(r'c\d+', table):
                    cur.execute(f"DROP TABLE {table}")
        conn.commit()
        conn.close()

def truncate(tables=[]):
    conn = _init_connection()
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
    conn.commit()
    conn.close()

    
