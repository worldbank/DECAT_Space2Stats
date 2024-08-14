import psycopg as pg
from psycopg_pool import ConnectionPool
from ..settings import Settings

settings = Settings()

conninfo = settings.DB_CONNECTION_STRING
pool = ConnectionPool(conninfo=conninfo, min_size=1, max_size=10, open=True)


def get_summaries(fields, h3_ids):
    colnames = ["hex_id"] + fields
    cols = [pg.sql.Identifier(c) for c in colnames]
    sql_query = pg.sql.SQL(
        """
            SELECT {0}
            FROM {1}
            WHERE hex_id = ANY (%s)
        """
    ).format(pg.sql.SQL(", ").join(cols), pg.sql.Identifier(settings.DB_TABLE_NAME))
    try:
        # Convert h3_ids to a list to ensure compatibility with psycopg
        h3_ids = list(h3_ids)
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql_query,
                    [
                        h3_ids,
                    ],
                )
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
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
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql_query,
                    [
                        settings.DB_TABLE_NAME,
                    ],
                )
                columns = [row[0] for row in cur.fetchall() if row[0] != "hex_id"]
    except Exception as e:
        raise e

    return columns