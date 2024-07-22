import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Read the existing Parquet file
input_file = 'space2stats.parquet'
output_file = 'space2stats_updated.parquet'

df = pd.read_parquet(input_file)

# Calculate the sum of male and female populations separately and combined
male_columns = [col for col in df.columns if col.startswith('sum_pop_m_')]
female_columns = [col for col in df.columns if col.startswith('sum_pop_f_')]

df['sum_pop_m_2020'] = df[male_columns].sum(axis=1)
df['sum_pop_f_2020'] = df[female_columns].sum(axis=1)
df['sum_pop_2020'] = df['sum_pop_m_2020'] + df['sum_pop_f_2020']

# Write the updated DataFrame back to a Parquet file
table = pa.Table.from_pandas(df)
pq.write_table(table, output_file)

print(f"Updated data saved to {output_file}")