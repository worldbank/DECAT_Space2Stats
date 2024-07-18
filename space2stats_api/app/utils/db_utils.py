import psycopg as pg

from ..settings import settings


DB_HOST = settings.DB_HOST
DB_PORT = settings.DB_PORT
DB_NAME = settings.DB_NAME
DB_USER = settings.DB_USER
DB_PASSWORD = settings.DB_PASSWORD
DB_TABLE_NAME = settings.DB_TABLE_NAME


def get_summaries(fields, h3_ids):
    h3_ids_str = ", ".join(f"'{h3_id}'" for h3_id in h3_ids)
    sql_query = f"""
    SELECT hex_id, {', '.join(fields)}
    FROM {DB_TABLE_NAME}
    WHERE hex_id IN ({h3_ids_str})
    """
    try:
        conn = pg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
    except Exception as e:
        raise e

    return rows, colnames


def get_available_fields():
    sql_query = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '{DB_TABLE_NAME}'
    """

    try:
        conn = pg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        columns = [row[0] for row in cur.fetchall() if row[0] != "hex_id"]
        cur.close()
        conn.close()
    except Exception as e:
        raise e

    return columns
