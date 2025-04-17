# Connecting Hexagons to Admin Bounds
While most of our work has focused on the consistent, global, hexagon grid, the ultimate goal of the S2S work program is to generate a database of geospatial aggregates at the administrative level 2. **We are still in the process of acquiring the admin bounds we are going to publish.**

Regardless we can plan the process of attaching the hexagons to the admin boundaries, even though it will need to be re-run when the boundaries are officially released. The following is the process we can take, with some questions thrown in:

1. Establish a map between official World Bank boundaries and our hexagon grid.  
  a. See notebook __ADM_map_adm2_h3xid.ipynb__ for examples of how it is being done now.  
  b. This will eventually pull from an ESRI rest service, something [like this](https://services.arcgis.com/iQ1dY19aHwbSDYIF/ArcGIS/rest/services/World_Bank_Official_Boundaries__World_International_Borders(Very_High_Definition)/FeatureServer).  
  c. Current results can be found on our S3 bucket __s3://wbg-geography01/Space2Stats/h3_spatial_data/ADM2_INTERSECTION_H3/__, see table below for example of the resulting country files:

 | gID | cell | overlap | cntry | 
 | --- | --- | --- | --- | 
 | ZWE_9357 | 869620a0fffffff | 0.1313096783678213 | ZWE |
 | ZWE_9357 | 869620a2fffffff | 0.031779878 | ZWE |
 | ZWE_9357 | 869620a47ffffff | 0.7878062320508961 | ZWE |

 2. Using these h3 overlaps, fetch the h3 results from the server and merge based on overlap percentages i.e - if a hexagon cell has a 0.5 overlap with a feature, that hexagon contributes 0.5 of its value to the adm2 sum).  
   a. See notebook __ADM_zonal_aggregations.ipynb__  
   b. There is still testing to be done to determine how many features can be requested by hex_id and fields from the server. It may also be faster to connect to the DB directly.
 
