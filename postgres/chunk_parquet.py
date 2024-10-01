import pandas as pd


df = pd.read_parquet('space2stats.parquet')
chunk_size = 100000  # Number of rows per chunk

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    chunk.to_parquet(f'parquet_chunks/space2stats_part_{i // chunk_size}.parquet')

print("Parquet file split into smaller chunks.")