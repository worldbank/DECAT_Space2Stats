## space2stats

### Generating Preliminary CATALOG, COLLECTION, and ITEM files
- Navigate to the METADATA sub-directory and run the following commands in order:
    1. get_types.py
    2. create_stac.py
- Note that the get types function is reading in a parquet file from the following directory: space2stats_api/src/local.parquet
- Here is a workflow diagram of the initial STAC metadata creation:

![Create Stac](../../docs/images/create_stac_workflow.png)

### Adding new ITEM files
- Navigate to the METADATA sub-directory
- In link_new_item.py set "Paths and metadata setup" in the main function to point towards the corresponding locally saved parquet file
- Run line_new_items.py