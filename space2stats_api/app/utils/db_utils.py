import os
import psycopg2
from dotenv import load_dotenv


load_dotenv("../db.env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")

print(DB_TABLE_NAME)

def get_summaries(fields, h3_ids):
    h3_ids_str = ', '.join(f"'{h3_id}'" for h3_id in h3_ids)
    sql_query = f"""
    SELECT hex_id, {', '.join(fields)}
    FROM {DB_TABLE_NAME}
    WHERE hex_id IN ({h3_ids_str})
    """

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
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
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        columns = [row[0] for row in cur.fetchall() if row[0]!='hex_id']
        print(columns)
        cur.close()
        conn.close()
    except Exception as e:
        raise e
    
    

    return columns