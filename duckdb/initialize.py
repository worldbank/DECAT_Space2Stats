import duckdb
from dotenv import load_dotenv
import s3fs

load_dotenv("wb_aws.env")

s3_bucket = 'wbg-geography01'
combined_file_path = 's3://wbg-geography01/Space2Stats/parquet/GLOBAL/combined_population.parquet'
s3 = s3fs.S3FileSystem()

duckdb_file = 'combined_population.duckdb'
con = duckdb.connect(duckdb_file)
con.execute(f"""
CREATE TABLE combined_population AS
SELECT * FROM read_parquet('{combined_file_path}', hive_partitioning=false);
""")


query_result = con.execute("SELECT * FROM combined_population LIMIT 5").fetchall()
print(query_result)