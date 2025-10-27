Python Client
=============

A Python client for accessing the Space2Stats API, providing easy access to consistent, comparable, and authoritative sub-national variation data from the World Bank.

Quick Start
-----------

Install the package using pip:

.. code-block:: bash

    pip install space2stats-client

Use the client to access the Space2Stats API:

.. code-block:: python

    from space2stats_client import Space2StatsClient
    import geopandas as gpd

    # Initialize the client
    client = Space2StatsClient()

    # Get available topics/datasets
    topics = client.get_topics()
    print(topics)

    # Get fields for a specific dataset
    fields = client.get_fields("dataset_id")
    print(fields)

    # Get data for an area of interest
    gdf = gpd.read_file("path/to/your/area.geojson")
    summary = client.get_summary(
        gdf=gdf,
        spatial_join_method="centroid",
        fields=["population", "gdp"]
    )

    # Get aggregated statistics
    aggregates = client.get_aggregate(
        gdf=gdf,
        spatial_join_method="centroid",
        fields=["population", "gdp"],
        aggregation_type="sum"
    )

.. autoclass:: space2stats_client.Space2StatsClient
    :members:
    :undoc-members:
    :show-inheritance:
    :member-order: bysource

Notebook Example
----------------

The following example demonstrates how to use the Space2Stats client in a Jupyter notebook:
:doc:`user-docs/space2stats_floods_with_client`
