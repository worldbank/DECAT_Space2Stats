import pandas as pd
import h3


# Load the full dataset
df = pd.read_parquet('space2stats.parquet')

# Define the bounding box for New York City (approximate values) as a GeoJSON polygon
nyc_polygon = {
    "type": "Polygon",
    "coordinates": [[
        [-74.259090, 40.477399],  
        [-73.700272, 40.477399],  
        [-73.700272, 40.917577],  
        [-74.259090, 40.917577],  
        [-74.259090, 40.477399]   
    ]]
}

# Generate H3 indices for the bounding box using polyfill
resolution = 6 
nyc_hexagons = h3.polyfill(nyc_polygon, resolution, geo_json_conformant=True)

# Filter the dataframe for New York City H3 indices
nyc_df = df[df['hex_id'].isin(nyc_hexagons)]

nyc_df.to_parquet('nyc_sample.parquet')

print("Filtered file for New York City.")