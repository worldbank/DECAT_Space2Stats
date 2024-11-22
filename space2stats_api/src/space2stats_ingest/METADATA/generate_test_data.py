import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Load the original Parquet file
input_file = "space2stats.parquet"
table = pq.read_table(input_file)

# Select only the 'hex_id' column
table = table.select(["hex_id"])

# Create the new 'test_column' with random values
num_rows = table.num_rows
test_column = pa.array(np.random.random(size=num_rows), type=pa.float64())

# Add 'test_column' to the table
table = table.append_column("test_column", test_column)

# Save the modified table to a new Parquet file
output_file = "space2stats_test.parquet"
pq.write_table(table, output_file)

print(f"Modified Parquet file saved as {output_file}")
