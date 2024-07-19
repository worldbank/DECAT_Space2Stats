import os
import re
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import s3fs
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv("../wb_aws.env")

s3_bucket = 'wbg-geography01'
input_prefix = 'Space2Stats/parquet/GLOBAL/WorldPop_2020_Demographics/'
output_file = f's3://{s3_bucket}/Space2Stats/parquet/GLOBAL/combined_population.parquet'

session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

s3_client = session.client('s3')

s3 = s3fs.S3FileSystem()

def extract_info(filename):
    match = re.match(r'([fm])_(\d+)_2020\.parquet', filename)
    if match:
        sex = match.group(1)
        age = match.group(2)
        return sex, age
    return None, None

@retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
def read_parquet_file(file_path):
    return pd.read_parquet(file_path, storage_options={'anon': False})

combined_df = pd.DataFrame()

response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=input_prefix)
files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]

for file in files:
    print(f"processing {file}")
    if file.endswith('.parquet'):
        sex, age = extract_info(file.split('/')[-1])
        if sex and age:
            df = read_parquet_file(f's3://{s3_bucket}/{file}')
            df = df.rename(columns={'SUM': f'sum_pop_{sex}_{age}_2020'})
            df = df[['id', f'sum_pop_{sex}_{age}_2020']]
            
            if combined_df.empty:
                combined_df = df
            else:
                combined_df = combined_df.merge(df, on='id', how='outer')

# Calculate the sum of male and female populations separately and combined
male_columns = [col for col in combined_df.columns if col.startswith('sum_pop_m_')]
female_columns = [col for col in combined_df.columns if col.startswith('sum_pop_f_')]

combined_df['sum_pop_m_2020'] = combined_df[male_columns].sum(axis=1)
combined_df['sum_pop_f_2020'] = combined_df[female_columns].sum(axis=1)
combined_df['sum_pop_2020'] = combined_df['sum_pop_m_2020'] + combined_df['sum_pop_f_2020']

combined_df = combined_df.rename(columns={'id': 'hex_id'})

table = pa.Table.from_pandas(combined_df)
pq.write_table(table, output_file, filesystem=s3)

print(f"combined data saved to {output_file}")