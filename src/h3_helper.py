import sys, os, importlib, json
import folium, shapely, rasterio, matplotlib

import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import geopandas as gpd

from rasterio.crs import CRS
from mpl_toolkits.axes_grid1 import make_axes_locatable
from h3 import h3
from shapely.geometry import Polygon, Point, mapping
from shapely.ops import unary_union
from urllib.request import urlopen
from tqdm import tqdm

import GOSTRocks.rasterMisc as rMisc
import GOSTRocks.ntlMisc as ntl
from GOSTRocks.misc import tPrint

def generate_h3_gdf(in_gdf, h3_level=7):
    ''' Generate a GeoDataFrame of h3 grid cells from an input geodataframe
    
        :param in_gdf: geodataframe from which to create h3 cells
        :type in_gdf: geopandas.GeoDataFrame
    '''
    try:
        del final_hexs
    except:
        pass

    try:
        final_hexs = list(h3.polyfill(in_gdf.unary_union.__geo_interface__, h3_level, geo_json_conformant=True))
    except:
        for cPoly in tqdm(in_gdf.unary_union, desc=f"Generating h3 grid level {h3_level}"):
            all_hexs = list(h3.polyfill(cPoly.__geo_interface__, h3_level, geo_json_conformant=True))
            try:        
                final_hexs = final_hexs + all_hexs
            except:
                final_hexs = all_hexs

            final_hexs = list(set(final_hexs))

    hex_poly = lambda hex_id: Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))
    all_polys = gpd.GeoSeries(list(map(hex_poly, final_hexs)), index=final_hexs, crs="EPSG:4326")
    all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=['geometry'])
    all_polys['shape_id'] = list(all_polys.index)
    return(all_polys)
    
def map_choropleth(sub, map_column, thresh=[], colour_ramp = 'Reds', invert=False, map_epsg=3857, legend_loc='upper right'):
        ''' generate a static map of variables in GeoDataFrame sub
            
            :param sub: GeoDataFrame with geometry and column to map
            :type sub: GeoPandas.GeoDataFrame
            :param map_column: Name of column in sub to map
            :type map_column: string
            :param thresh: list of values to classify data in map_column
            :type thresh: list of ints
        '''
        try:
            sub = sub.to_crs(map_epsg)
        except:
            sub.crs = 4326
            sub = sub.to_crs(map_epsg)

        thresh=[]
        map_sub = sub.copy()
        cmap = matplotlib.cm.get_cmap(colour_ramp)
        fig, ax = plt.subplots(figsize=(15,15))
        proj = CRS.from_epsg(map_epsg)

        # create map column in sub, based on re-mapping of column map_column
        if len(thresh) == 0:
            split = [0,0.2,0.4,0.6,0.8,1]
            thresh = [x for x in map_sub[map_column].quantile(split).values]
            thresh.insert(0,0)

        map_sub['map'] = pd.cut(map_sub[map_column], thresh, labels=list(range(0, len(thresh)-1)))

        # [x/max(thresh) for x in thresh]
        cmap_divisions = [x/100 for x in list(range(0,101,20))]
        # map features not included in grouping
        sel_mixed = map_sub.loc[map_sub['map'].isna()]
        mismatch_color = 'azure'
        mismatch_edge = 'darkblue'
        cur_patch = mpatches.Patch(facecolor=mismatch_color, edgecolor=mismatch_edge, hatch="///", label=f"Mismatch [{sel_mixed.shape[0]}]")
        all_labels = [cur_patch]
        for lbl, data in map_sub.groupby('map'):
            cur_color = cmap(cmap_divisions[int(lbl)])
            if invert:
                cur_color = cmap(1 - cmap_divisions[int(lbl)])
            data.plot(color=cur_color, ax=ax, linewidth=0.1)
            cur_patch = mpatches.Patch(color=cur_color, label=f'{data[map_column].min()} - {data[map_column].max()} [{data.shape[0]}]')
            all_labels.append(cur_patch)

        sel_mixed.plot(color=mismatch_color, edgecolor=mismatch_edge, hatch="//////", ax=ax, label=False, linewidth=2)
            
        ctx.add_basemap(ax, source=ctx.providers.Stamen.TonerBackground, crs=proj) #zorder=-10, 'EPSG:4326'
        ax.legend(handles=all_labels, loc=legend_loc)        
        ax = ax.set_axis_off()

        return(ax)
    
def static_map_h3(sub, map_epsg=3857, legend_loc='upper right'):
        ''' generate a static map of the h3 grid in sub
        '''
        try:
            sub = sub.to_crs(map_epsg)
        except:
            sub.crs = 4326
            sub = sub.to_crs(map_epsg)
        
        fig, ax = plt.subplots(figsize=(15,15))
        proj = CRS.from_epsg(map_epsg)
        
        sub.plot(color='grey', ax=ax, linewidth=0.1)
            
        ctx.add_basemap(ax, source=ctx.providers.Stamen.TonerBackground, crs=proj) #zorder=-10, 'EPSG:4326'
        ax = ax.set_axis_off()
        return(ax)