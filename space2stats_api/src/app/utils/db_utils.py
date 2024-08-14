import psycopg as pg
from ..settings import Settings

settings = Settings()

DB_HOST = settings.DB_HOST
DB_PORT = settings.DB_PORT
DB_NAME = settings.DB_NAME
DB_USER = settings.DB_USER
DB_PASSWORD = settings.DB_PASSWORD
DB_TABLE_NAME = settings.DB_TABLE_NAME or "space2stats"


def get_summaries(fields, h3_ids):
    colnames = ["hex_id"] + fields
    cols = [pg.sql.Identifier(c) for c in colnames]
    sql_query = pg.sql.SQL(
        """
            SELECT {0}
            FROM {1}
            WHERE hex_id = ANY (%s)
        """
    ).format(pg.sql.SQL(", ").join(cols), pg.sql.Identifier(DB_TABLE_NAME))
    try:
        conn = pg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute(
            sql_query,
            [
                h3_ids,
            ],
        )
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
    except Exception as e:
        raise e

    return rows, colnames


def get_available_fields():
    sql_query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = %s
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
        cur.execute(
            sql_query,
            [
                DB_TABLE_NAME,
            ],
        )
        columns = [row[0] for row in cur.fetchall() if row[0] != "hex_id"]
        cur.close()
        conn.close()
    except Exception as e:
        raise e

    return columns
