## space2stats

### Generating STAC files
- Navigate to the METADATA sub-directory and run the following commands in order:
    1. get_types.py
    2. create_stac.py
- Note that the get types function is reading in a parquet file from the following directory: space2stats_api/src/local.parquet
- Here is a workflow diagram of the STAC metadata creation:

![Create Stac](../../docs/images/create_stac_workflow.png)