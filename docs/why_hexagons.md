# Space2Stats and Hexagons
Space2Stats aggregates data to a globally consistent hexagon grid, leveraging [Uber’s Hexagonal Hierarchical Spatial Index](https://www.uber.com/blog/h3/) ([Github link here](https://github.com/uber/h3)). Hexagons have several benefits over traditional grids, but it is not all that common a toolset, and not an obvious choice to many Geographers, let alone non-geospatial experts. This page is focused on explaining our decision process that led us to these hexagons.

## Consistent shapes over administrative divisions
The goal of the Space2Stats program is to produce a global database of geospatial aggregates at administrative 2 level, so why do we initially aggregate to a consistent grid before attaching the variables to the administrative boundaries? Simply put, administrative boundaries change constantly, which would require re-calculating the entire database with every change. This changing landscape is well-known and there are many projects that are attempting to collect updated administrative divisions:

1. [UN-SALB](https://salb.un.org/en): The Second Administrative Level Boundaries (SALB) programme objective is to promote accessible, interoperable and global data and information on subnational units and boundaries, or common geographies, for better decisions, stronger support to people and planet and to monitor the Sustainable Development Goals.
2. [GeoBoundaries](https://www.geoboundaries.org/): The geoBoundaries Global Database of Political Administrative Boundaries Database is an online, open license (CC BY 4.0) resource of information on administrative boundaries (i.e., state, county) for every country in the world. Since 2016, we have tracked approximately 1 million boundaries within over 200 entities, including all UN member states.
3. [FAO GAUL](https://www.fao.org/agroinformatics/news/news-detail/now-available--the-global-administrative-unit-layers-(gaul)-dataset---2024-edition/en): In 2024, FAO released an updated version of the Global Administrative Unit Layers (GAUL) dataset al level 1 and 2, as part of its ongoing efforts to strengthen the geospatial foundation of global food and agriculture systems. 
4. [GADM](https://gadm.org/index.html): GADM provides maps and spatial data for all countries and their sub-divisions

The Space2Stats program will also publish a global database of administrative boundaries at level 0, 1, and 2, in order to comply with the World Bank's strict legal requirements on international boundaries.

## Hexagons vs Grids
Once we acknowledge the necessity of a standard grid for aggregation, why did we choose hexagons over a grid? There are two principal advantages to using hexagons:
1. Hexagons have a consistent area across the globe, unlike grids which change in width as you move north or south from the equator.
2. Hexagons have more consistent neighbour calculations than grids or triangles.

```{figure} images/hexagon_neighbours.jpg
---
alt: Example of neighbour calculations for various shape tessellations
---
Example of neighbour calculations for tessellations of triangle, hexagons and squares. Image taken from [Uber H3 website](https://www.uber.com/blog/h3/)
```

Additionally, several World Bank projects are leveraging hexagons in their data indexing and calculations:

1. [World Ex](https://worldbank.github.io/worldex/): A python package for indexing geospatial data with the h3 index
2. Food security (Link TBD): TBD
