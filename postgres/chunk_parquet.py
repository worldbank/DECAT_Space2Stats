import os

import pandas as pd

chunk_dir = "parquet_chunks"
df = pd.read_parquet('space2stats_updated.parquet')
chunk_size = 100000  # Number of rows per chunk

if not os.path.exists(chunk_dir):
    os.mkdir(chunk_dir)

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    chunk.to_parquet(os.path.join(chunk_dir, f'space2stats_part_{i // chunk_size}.parquet'))

print("Parquet file split into smaller chunks.")