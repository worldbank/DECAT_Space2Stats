import os
import pickle
import sys
from urllib.request import urlopen

import contextily as ctx
import geopandas as gpd
import h3
import matplotlib
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd

from GOSTrocks.misc import tPrint
from mpl_toolkits.axes_grid1 import make_axes_locatable
from rasterio.crs import CRS
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union
from tqdm import tqdm


def generate_h3_gdf(in_gdf, h3_level=7):
    """Generate a GeoDataFrame of h3 grid cells from an input geodataframe

    :param in_gdf: geodataframe from which to create h3 cells
    :type in_gdf: geopandas.GeoDataFrame
    """
    try:
        del final_hexs
    except:
        pass

    try:
        final_hexs = list(
            h3.polyfill(
                in_gdf.unary_union.__geo_interface__, h3_level, geo_json_conformant=True
            )
        )
    except:
        for cPoly in tqdm(
            in_gdf.unary_union, desc=f"Generating h3 grid level {h3_level}"
        ):
            all_hexs = list(
                h3.polyfill(cPoly.__geo_interface__, h3_level, geo_json_conformant=True)
            )
            try:
                final_hexs = final_hexs + all_hexs
            except:
                final_hexs = all_hexs

            final_hexs = list(set(final_hexs))

    hex_poly = lambda hex_id: Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))
    all_polys = gpd.GeoSeries(
        list(map(hex_poly, final_hexs)), index=final_hexs, crs="EPSG:4326"
    )
    all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=["geometry"])
    all_polys["shape_id"] = list(all_polys.index)
    return all_polys


def generate_lvl0_lists(
    h3_lvl,
    return_gdf=False,
    buffer0=False,
    read_pickle=True,
    pickle_file="h0_dictionary_of_h{lvl}_geodata_frames.pickle",
):
    """generate a dictionary with keys as lvl0 codes with all children at h3_lvl level as values

    Parameters
    ----------
    h3_lvl : int
        h3 level to generate children of h0 parents
    return_gdf : bool, optional
        return a GeoDataFrame instead of a dictionary, by default False
    buffer0 : bool, optional
        buffer the h3 lvl 0 cells by 0 to fix inherent topological errors, by default False
    read_pickle : bool, optional
        Optionally choose the read resulting data from a [ickle file defined by pickle_file, by default True. If pickle
        file is not present, function will continue to generate results as if flag was set to False
    pickle_file : str, optional
        Path of pickle file to read if read_pickle is set to True

    Returns
    -------
    dict
        dictionary with keys as lvl0 codes with all children at h3_lvl level as values; returns a GeoDataFrame if return_gdf is True
    """
    if read_pickle:
        try:
            pickle_file = pickle_file.format(lvl=h3_lvl)
            pickle_path = os.path.join(
                os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__))),
                pickle_file,
            )
            print(
                f"Loading pickle file {pickle_file}: it exists {os.path.exists(pickle_path)}"
            )
            with open(pickle_path, "rb") as handle:
                xx = pickle.load(handle)
            return xx
        except:
            # print("Could not load pickle file, continuing to process h0 manually")
            raise (ValueError("Could not load pickle file"))

    # Get list of all h3 lvl 0 cells
    h3_lvl0 = list(h3.get_res0_cells())

    # Generate list of all children of h3 lvl 0 cells
    h3_lvl0_children = {}
    for h3_0 in h3_lvl0:
        h3_children = list(h3.h3_to_children(h3_0, h3_lvl))
        if return_gdf:
            hex_poly = lambda hex_id: Polygon(
                h3.h3_to_geo_boundary(hex_id, geo_json=True)
            )
            all_polys = gpd.GeoSeries(
                list(map(hex_poly, h3_children)), index=h3_children, crs=4326
            )
            all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=["geometry"])
            if buffer0:
                all_polys["geometry"] = all_polys["geometry"].apply(
                    lambda x: x.buffer(0)
                )
            all_polys["shape_id"] = list(all_polys.index)

            h3_lvl0_children[h3_0] = all_polys
        else:
            h3_lvl0_children[h3_0] = h3_children
    return h3_lvl0_children


def generate_lvl1_lists(
    h3_lvl,
    return_gdf=False,
    buffer0=False,
    read_pickle=True,
    pickle_file="h1_dictionary_of_h{lvl}_geodata_frames.pickle",
    write_pickle=False,
):
    """generate a dictionary with keys as lvl1 codes with all children at h3_lvl level as values

    Parameters
    ----------
    h3_lvl : int
        h3 level to generate children of h0 parents
    return_gdf : bool, optional
        return a GeoDataFrame instead of a dictionary, by default False
    buffer0 : bool, optional
        buffer the h3 lvl 0 cells by 0 to fix inherent topological errors, by default False
    read_pickle : bool, optional
        Optionally choose the read resulting data from a [ickle file defined by pickle_file, by default True. If pickle
        file is not present, function will continue to generate results as if flag was set to False
    pickle_file : str, optional
        Path of pickle file to read if read_pickle is set to True

    Returns
    -------
    dict
        dictionary with keys as lvl0 codes with all children at h3_lvl level as values; returns a GeoDataFrame if return_gdf is True
    """
    pickle_file = pickle_file.format(lvl=h3_lvl)
    pickle_path = os.path.join(
        os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__))),
        pickle_file,
    )
    if read_pickle:
        try:
            print(
                f"Loading pickle file {pickle_file}: it exists {os.path.exists(pickle_path)}"
            )
            with open(pickle_path, "rb") as handle:
                xx = pickle.load(handle)
            return xx
        except:
            print("Could not load pickle file, continuing to process h1 manually")
            raise (
                ValueError(
                    "Could not load pickle file, exiting. Set read_pickle to False to generate list"
                )
            )

    # Get list of all h3 lvl 0 cells
    h3_lvl0 = list(h3.get_res0_cells())

    # Generate list of all children of h3 lvl 1 cells
    h3_lvl1_children = {}
    for h3_0 in h3_lvl0:  # Identify all lvl 0 cells
        h3_children = list(h3.cell_to_children(h3_0, 1))
        for (
            h3_1
        ) in h3_children:  # For current lvl 0 cell, loop through all level 1 children
            h3_children_1 = list(h3.cell_to_children(h3_1, h3_lvl))
            if return_gdf:
                hex_poly = lambda hex_id: Polygon(
                    h3.cell_to_boundary(hex_id)
                )
                all_polys = gpd.GeoSeries(
                    list(map(hex_poly, h3_children_1)), index=h3_children_1, crs=4326
                )
                all_polys = gpd.GeoDataFrame(all_polys, crs=4326, columns=["geometry"])
                if buffer0:
                    all_polys["geometry"] = all_polys["geometry"].apply(
                        lambda x: x.buffer(0)
                    )
                all_polys["shape_id"] = list(all_polys.index)

                h3_lvl1_children[h3_1] = all_polys
            else:
                h3_lvl1_children[h3_1] = h3_children_1

    if write_pickle:
        if not os.path.exists(pickle_path):
            with open(pickle_path, "wb") as handle:
                pickle.dump(h3_lvl1_children, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return h3_lvl1_children


def map_choropleth(
    sub,
    map_column,
    thresh=[],
    colour_ramp="Reds",
    invert=False,
    map_epsg=3857,
    legend_loc="upper right",
):
    """generate a static map of variables in GeoDataFrame sub

    :param sub: GeoDataFrame with geometry and column to map
    :type sub: GeoPandas.GeoDataFrame
    :param map_column: Name of column in sub to map
    :type map_column: string
    :param thresh: list of values to classify data in map_column
    :type thresh: list of ints
    """
    try:
        sub = sub.to_crs(map_epsg)
    except:
        sub.crs = 4326
        sub = sub.to_crs(map_epsg)

    thresh = []
    map_sub = sub.copy()
    cmap = matplotlib.cm.get_cmap(colour_ramp)
    fig, ax = plt.subplots(figsize=(15, 15))
    proj = CRS.from_epsg(map_epsg)

    # create map column in sub, based on re-mapping of column map_column
    if len(thresh) == 0:
        split = [0, 0.2, 0.4, 0.6, 0.8, 1]
        thresh = [x for x in map_sub[map_column].quantile(split).values]
        thresh.insert(0, 0)

    map_sub["map"] = pd.cut(
        map_sub[map_column], thresh, labels=list(range(0, len(thresh) - 1))
    )

    # [x/max(thresh) for x in thresh]
    cmap_divisions = [x / 100 for x in list(range(0, 101, 20))]
    # map features not included in grouping
    sel_mixed = map_sub.loc[map_sub["map"].isna()]
    mismatch_color = "azure"
    mismatch_edge = "darkblue"
    cur_patch = mpatches.Patch(
        facecolor=mismatch_color,
        edgecolor=mismatch_edge,
        hatch="///",
        label=f"Mismatch [{sel_mixed.shape[0]}]",
    )
    all_labels = [cur_patch]
    for lbl, data in map_sub.groupby("map"):
        cur_color = cmap(cmap_divisions[int(lbl)])
        if invert:
            cur_color = cmap(1 - cmap_divisions[int(lbl)])
        data.plot(color=cur_color, ax=ax, linewidth=0.1)
        cur_patch = mpatches.Patch(
            color=cur_color,
            label=f"{data[map_column].min()} - {data[map_column].max()} [{data.shape[0]}]",
        )
        all_labels.append(cur_patch)

    sel_mixed.plot(
        color=mismatch_color,
        edgecolor=mismatch_edge,
        hatch="//////",
        ax=ax,
        label=False,
        linewidth=2,
    )

    ctx.add_basemap(
        ax, source=ctx.providers.Stamen.TonerBackground, crs=proj
    )  # zorder=-10, 'EPSG:4326'
    ax.legend(handles=all_labels, loc=legend_loc)
    ax = ax.set_axis_off()

    return ax


def static_map_h3(sub, map_epsg=3857, legend_loc="upper right"):
    """generate a static map of the h3 grid in sub"""
    try:
        sub = sub.to_crs(map_epsg)
    except:
        sub.crs = 4326
        sub = sub.to_crs(map_epsg)

    fig, ax = plt.subplots(figsize=(15, 15))
    proj = CRS.from_epsg(map_epsg)

    sub.plot(color="grey", ax=ax, linewidth=0.1)

    ctx.add_basemap(
        ax, source=ctx.providers.Stamen.TonerBackground, crs=proj
    )  # zorder=-10, 'EPSG:4326'
    ax = ax.set_axis_off()
    return ax

