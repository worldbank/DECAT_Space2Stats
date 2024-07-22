import sys, os, importlib, json
import folium, shapely, rasterio, matplotlib

import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import geopandas as gpd
import numpy as np

from rasterio.crs import CRS
from mpl_toolkits.axes_grid1 import make_axes_locatable
from h3 import h3
from shapely.geometry import Polygon, Point, mapping
from shapely.ops import unary_union
from urllib.request import urlopen
from tqdm import tqdm

import h3_helper
import GOSTrocks.rasterMisc as rMisc
import GOSTrocks.ntlMisc as ntl
from GOSTrocks.misc import tPrint

def calculate_value(in_shp, zonal_res, h3_level, feat_id, fractional_res=True, 
                    zonal_res_id='shape_id', default_sum='SUM'):
    ''' tabulate hexabin stats for all bins that intersect shape in_shp

    :param in_shp: shape of boundary to intersect with hexabins
    :type in_shp: shapely polygon
    :param zonal_res: DataFrame of h3 results to search through
    :type zonal_res: pandas.DataFrame
    :param h3_level: level of h3 grid to search for
    :type h3_level: int
    :param fractional_res: should admin intersection with h3 account for fractional overlaps, default is True
    :type fractional_res: Boolean, optional
    :param zonal_res_id: id column in zonal_res thta contains hex ids, defaults to "shape_id"
    :type zonal_res_id: string, optional
    :param default_sum: default function for aggregating intersecting hexs if arg is not in coulmn name;
                        function sill pick up [SUM,MIN,MAX,MEAN], defaults to sum
    :type default_sum: string, optional


    :return: dictionary of results summarized based on type (SUM, MIN, MEAN, MAX)
    :rtype: Dictionary
    '''
    def get_intersection(admin_shp, hex_shp):
        ''' get fraction of hex_shp that is inside admin_shp
        '''
        if admin_shp.contains(hex_shp):
            return(1)
        else:
            return(admin_shp.intersection(hex_shp).area/hex_shp.area)
    
    res = {'id':feat_id}
    process_h3 = True
    # Generate h3 cells that intersect current shape; if none are generated first time through, buffer
    #  the geometry by a little bit, and then search again
    while process_h3:        
        if in_shp.geom_type == "Polygon":
            sel_h3 = h3.polyfill(in_shp.__geo_interface__, h3_level, geo_json_conformant=True)
        elif in_shp.geom_type == "MultiPolygon":
            for cPoly in in_shp:
                temp_h3 = h3.polyfill(cPoly.__geo_interface__, h3_level, geo_json_conformant=True)
                try:
                    sel_h3 = sel_h3.union(temp_h3)
                except:
                    sel_h3 = temp_h3
        process_h3 = len(sel_h3) == 0
        in_shp = in_shp.buffer(0.1)

    if len(sel_h3) > 0:
        hex_poly = lambda hex_id: Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))
        all_polys = gpd.GeoSeries(list(map(hex_poly, sel_h3)), index=sel_h3, crs="EPSG:4326")
        all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=['geometry'])
        all_polys['shape_id'] = list(all_polys.index)
        if fractional_res:
            all_polys['inter_area'] = all_polys['geometry'].apply(lambda x: get_intersection(in_shp, x))
        else:
            all_polys['inter_area'] = 1        
        all_polys = pd.merge(all_polys, zonal_res, left_on='shape_id', right_on=zonal_res_id)
        for col in all_polys.columns: 
            if not col in ['inter_area','geometry','shape_id']: 
                calc_type = default_sum
                if "SUM" in col: calc_type = "SUM"
                if "MIN" in col: calc_type = "MIN"
                if "MAX" in col: calc_type = "MAX"
                if "MEAN" in col: calc_type = "MEAN"            
                try:
                    if calc_type == "SUM": # For sum columns, multiply column by inter_area and sum results
                        cur_val = sum(all_polys[col] * all_polys['inter_area'])
                    elif calc_type == "MIN":
                        cur_val = all_polys[col].min()
                    elif calc_type == "MAX":
                        cur_val = all_polys[col].max()
                    elif calc_type == "MEAN":
                        cur_val = sum(all_polys[col] * all_polys['inter_area'])/sum(all_polys['inter_area'])
                    res[col] = cur_val
                except:
                    pass
                try:
                    del(cur_val)
                except:
                    pass
    else:
        pass
    return(res)

def connect_polygons_h3_stats(inA, stats_df, h3_level, id_col, fractional_res=True,
                              zonal_res_id='shape_id', default_sum='SUM'):
    ''' merge stats from hexabin stats dataframe (stats_df) with the inA geodataframe
    
    :param inA: input boundary dataset
    :type inA: geopandas.GeoDataFrame
    :param stats_df: input hexabin stats dataset
    :type stats_df: pandas.DataFrame
    :param h3_level: h3 hex level
    :type h3_level: int
    :param id_col: column in inA that uniquely identifies rows
    :type id_col: string
    :param zonal_res_id: id column in zonal_res thta contains hex ids, defaults to "shape_id"
    :type zonal_res_id: string, optional
    :param default_sum: default function for aggregating intersecting hexs if arg is not in coulmn name;
                        function sill pick up [SUM,MIN,MAX,MEAN], defaults to sum
    :type default_sum: string, optional

    
    :return: pandas dataframe with attached statistics and matching id from id_col
    :rtype: geopandas.GeoDataFrame
    '''
    all_res = []
    for idx, row in inA.iterrows():               
        try:
            all_res.append(calculate_value(row['geometry'], stats_df, h3_level, row[id_col], fractional_res, zonal_res_id, default_sum))
        except:
            print(f'Error processing {idx}')        
        
    return(pd.DataFrame(all_res))
    
class country_h3_zonal():
    ''' Generate h3 grid at prescribed level; intersect with admin boundary; run zonal stats
    
        :param iso3: Country ISO3 code
        :type iso3: string
        :param adm_bounds: admin boundaries for joining with h3 grid
        :type adm_bounds: geopandas.GeoDataFrame
        :param adm_bounds_id: column in adm_bounds used as unique ID
        :type adm_bounds_id: string
        :param h3_level: size of h3 grid to create; we suggest starting with 6 or 5 (5 is larger)
        :type h3_level: int
    '''
    def __init__(self, iso3, adm_bounds, adm_bounds_id, h3_level, out_folder, h3_grid = ''):
        self.iso3 = iso3
        self.adm_bounds = adm_bounds
        self.adm_bounds_id = adm_bounds_id
        self.h3_level = h3_level
        self.out_folder = out_folder
        
        #define output variables
        if h3_grid != '':
            self.out_h3_grid = os.path.join(out_folder, f'h3_level_{h3_level}.geojson')
        else:
            self.out_h3_grid = h3_grid
        try:
            self.h3_cells = gpd.read_file(self.out_h3_grid)
        except:
            pass
        self.out_admin = os.path.join(out_folder, 'admin_bounds.geojson')
        try:
            self.adm_bounds_h3 = gpd.read_file(self.out_admin)
        except:
            pass
        
        
    def generate_h3_grid(self, cols_to_include=[], attach_admin=False):
        ''' Generate the h3 grid and join to the admin boundaries
            
            :param cols_to_include: list of columns to include from adm_bounds in joined output
            :type cols_to_include: list of strings
        '''
        selA = self.adm_bounds
        try:
            return(self.h3_cells)
        except: 
            pass
        
        try:
            h3_cells = self.h3_cells.copy()
            h3_cells = h3_cells.loc[:,['shape_id','geometry']]
        except:
            h3_cells = h3_helper.generate_h3_gdf(self.adm_bounds, self.h3_level)
        
        h3_cells['centroid'] = h3_cells['geometry'].apply(lambda x: x.centroid)                
        h3_centroids = h3_cells.set_geometry('centroid')
        cols_to_include.append("geometry")
        cols_to_include.append(self.adm_bounds_id)
        cols_to_include = list(set(cols_to_include))
        h3_joined = gpd.sjoin(h3_centroids, selA.loc[:,cols_to_include], how='left')        
        if attach_admin:        
            h3_pivot = pd.pivot_table(h3_joined, index=self.adm_bounds_id, aggfunc={cols_to_include[0]:len})        
            h3_pivot.columns = [*h3_pivot.columns[:-1], 'h3_count']
            h3_pivot = h3_pivot.reset_index()
            h3_pivot = selA.loc[:,cols_to_include].merge(h3_pivot, how='left', on=self.adm_bounds_id)
            self.adm_bounds_h3 = h3_pivot

        h3_joined = h3_joined.set_geometry("geometry").drop(['centroid'], axis=1)
        h3_joined = h3_joined.reset_index()
        self.h3_cells = h3_joined
        
        return(h3_joined)
        
    def summarize_adm_h3_join(self, verbose=False):
        ''' Summarize the join between the adm bounds and the h3 grid:
            1. Number of h3 cells
            2. Number of adm bounds
            3. Number of adm bounds with 0 h3 centroids
            4. Number of adm bounds with 0 - 1 h3 centroids
            5. Number of adm bounds with 2 - 5 h3 centroids            
        '''
        try:
            inD = self.adm_bounds_h3.copy()
        except:
            self.generate_h3_grid()
            inD = self.adm_bounds_h3.copy()
        
        n_h3 = self.h3_cells.shape[0]
        n_adm = inD.shape[0]
        n_adm_0 = inD.loc[inD['h3_count'].isna()].shape[0]
        n_adm_1 = inD.loc[inD['h3_count'] == 1].shape[0]
        n_adm_2 = inD.loc[(inD['h3_count'] < 6) & (inD['h3_count'] > 1)].shape[0]
        
        if verbose:
            tPrint(f"{self.iso3}: H3 [{n_h3}], ADM [{n_adm}], ADM0 [{n_adm_0}], ADM1 [{n_adm_1}], ADM2 [{n_adm_2}]")
        return([n_h3, n_adm, n_adm_0, n_adm_1, n_adm_2])
        
    def write_output(self, write_h3=True, write_admin=False):
        ''' write geospatial data to disk
        
        '''
        if write_h3:
            self.h3_cells.to_file(self.out_h3_grid, driver="GeoJSON")
        if write_admin:
            self.adm_bounds_h3.to_file(self.out_admin, driver="GeoJSON")

    def zonal_raster(self, in_raster, minVal='', maxVal='', all_touched=False, weighted=False):
        '''

            :param in_raster: string path to raster file for calculations
            :type in_raster: string
            :param minVal: minimum value in in_raster to pass to zonal function; everything below is considered 0. Default is no threshold
            :type minVal: numeric
        '''
        h3_grid = self.generate_h3_grid()
        
        if isinstance(in_raster, str):
            inR = rasterio.open(in_raster, 'r')
        else:
            inR = in_raster
        
        # Run zonal statistics on pop_raster
        res = rMisc.zonalStats(h3_grid, inR, reProj=True, minVal=minVal, maxVal=maxVal,
                                allTouched=all_touched, weighted=weighted)
        res = pd.DataFrame(res, columns=["SUM", "MIN", "MAX", "MEAN"])            
        res['shape_id'] = h3_grid['shape_id'].astype(object)
        return(res)
            

    
    def zonal_raster_population(self, in_raster, pop_raster, raster_thresh, thresh_label='thresh',
                    resampling_type="sum", minVal='', maxVal='', all_touched=False, weighted=False):
        ''' extract raster data from in_raster, urban_raster for selected country, standardize urban_raster to in_raster

            :param in_raster: string path to raster file for calculations
            :type in_raster: string
            :param pop_raster: string path to population file for summarizing calculations
            :type pop_raster: string 
            :param raster_thresh: value to threshold in_raster in order to summarize population
            :type raster_thresh: number

            :param thresh_label: label to append to thresholded summaries in output table, default is to 'thresh'
            :type thresh_label: string
            :param resampling_type: how to re-sample in_raster to pop_raster, using rasterio resampling options, default is to 'SUM'         
            :type resampling_type: string
            :param minVal: minimum value in in_raster to pass to zonal function; everything below is considered 0. Default is no threshold
            :type minVal: numeric
            :param urban_mask_val: list of values in urban_raster to be used for mask
            :type urban_mask_val: list of int
            :param unqVals:
            :type unqVals:


        '''
        h3_grid = self.generate_h3_grid()
        
        if isinstance(in_raster, str):
            inR = rasterio.open(in_raster, 'r')
        else:
            inR = in_raster
        if isinstance(pop_raster, str):
            popR = rasterio.open(pop_raster, 'r')
        else:
            popR = pop_raster

        # Clip pop_r to extent of country
        inN, profile1 = rMisc.clipRaster(popR, self.adm_bounds, crop=False)

        # Clip inR to extent of country
        inD, profile2 = rMisc.clipRaster(inR, self.adm_bounds, crop=False)

        with rMisc.create_rasterio_inmemory(profile1, inN) as tempR:
            # Run zonal statistics on pop_raster
            res = rMisc.zonalStats(h3_grid, tempR, reProj=True, minVal=minVal, maxVal=maxVal,
                                    allTouched=all_touched, weighted=weighted)
            res = pd.DataFrame(res, columns=["SUM", "MIN", "MAX", "MEAN"])            
            res['shape_id'] = h3_grid['shape_id'].astype(object)            
            # Standardize in_raster to pop_raster
            with rMisc.create_rasterio_inmemory(profile2, inD) as tempD:
                inD, profile2 = rMisc.standardizeInputRasters(tempD, tempR, resampling_type=resampling_type)

                # threhsold in raster to create binary
                inR_thresh = (inD >= raster_thresh)
                pop_thresh = inN * inR_thresh
                
                # Summarize thresholded populatino
                with rMisc.create_rasterio_inmemory(profile1, pop_thresh) as urbanR:
                    resU = rMisc.zonalStats(h3_grid, urbanR, reProj=True, minVal=minVal, maxVal=maxVal,
                                            allTouched=all_touched, weighted=weighted)
                    resU = pd.DataFrame(resU, columns=[f"SUM_{thresh_label}", f"MIN_{thresh_label}", f"MAX_{thresh_label}", f"MEAN_{thresh_label}"])
                    resU = resU.astype(float)
                    resU['shape_id'] = h3_grid['shape_id']
                res_final = res.merge(resU, on='shape_id')        
        return res_final



    def zonal_raster_urban(self, in_raster, urban_raster, resampling_type="nearest", minVal='', maxVal='', rastType='N',
                           urban_mask_val=[21,22,23,30], unqVals=[], all_touched=False, weighted=False):
        ''' extract raster data from in_raster, urban_raster for selected country, standardize urban_raster to in_raster

            :param in_raster: string path to raster file for calculations
            :type in_raster: string
            :param in_raster: string path to urban file tiering calculations
            :type in_raster: string            
            :param minVal: minimum value in in_raster to pass to zonal function; everything below is considered 0. Default is no threshold
            :type minVal: numeric
            :param rastType: define the input data in the in_raster. Options are N (for numeric, default) or C (categorical)
            :type rastType: string
            :param urban_mask_val: list of values in urban_raster to be used for mask
            :type urban_mask_val: list of int
            :param unqVals:
            :type unqVals:
            
            :return: dictionary of rasterio objects for in_raster and urban_raster
            :rtype: dictionary of 'in_raster': rasterio.DatasetReader, 'urban_raster': rasterio.DatasetReader                    
        '''        
        h3_grid = self.generate_h3_grid()
        
        if isinstance(in_raster, str):
            inR = rasterio.open(in_raster, 'r')
        else:
            inR = in_raster
        
        if isinstance(urban_raster, str):
            inU = rasterio.open(urban_raster, 'r')
        else:
            inU = urban_raster

        # Clip inR to extent of country
        inN, profile1 = rMisc.clipRaster(inR, self.adm_bounds, crop=False)
        with rMisc.create_rasterio_inmemory(profile1, inN) as tempR:
            if rastType == 'N':
                # Run zonal statistics on in_raster
                res = rMisc.zonalStats(h3_grid, tempR, rastType=rastType, reProj=True, minVal=minVal, maxVal=maxVal,
                                       allTouched=all_touched, weighted=weighted)
                res = pd.DataFrame(res, columns=["SUM", "MIN", "MAX", "MEAN"])            
                res['shape_id'] = h3_grid['shape_id'].astype(object)            
                # Standardize in_urban raster to clippedR
                outU, profile2 = rMisc.standardizeInputRasters(inU, tempR, resampling_type=resampling_type)
                # Isolate values in in_raster that are urban
                inN_urban = np.isin(outU, urban_mask_val)  
                with rMisc.create_rasterio_inmemory(profile2, inN_urban) as urbanR:
                    resU = rMisc.zonalStats(h3_grid, urbanR, rastType=rastType, reProj=True, minVal=minVal, maxVal=maxVal,
                                            allTouched=all_touched, weighted=weighted)
                    resU = pd.DataFrame(resU, columns=["SUM_urban", "MIN_urban", "MAX_urban", "MEAN_urban"])
                    resU = resU.astype(float)
                    resU['shape_id'] = h3_grid['shape_id']
                res_final = res.merge(resU, on='shape_id')        
            elif rastType == 'C':
                # Run zonal statistics on in_raster
                res_final = rMisc.zonalStats(h3_grid, tempR, rastType=rastType, reProj=True, unqVals=unqVals,
                                             allTouched=all_touched, weighted=weighted)
                res_final = pd.DataFrame(res_final, columns=[f'c_x' for x in unqVals])            
                res_final['shape_id'] = h3_grid['shape_id'].astype(object)   

        return(res_final)
            
                
        