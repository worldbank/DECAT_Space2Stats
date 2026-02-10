import rasterio

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from affine import Affine
from rasterio.features import rasterize, MergeAlg

def map_flood(mapD, return_period, out_file):
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 6))
    flood_columns = [f"frac_area_flooded_CU_{return_period}yr", f"frac_area_flooded_FU_{return_period}yr", f"frac_area_flooded_PD_{return_period}yr"]
    flood_titles = ["Coastal", "Fluvial", "Pluvial"]
    flood_thresh = [0, 1, 3, 5, 10, 100]
    i = 0

    for col in flood_columns:
        ax = axes[i]
        if i ==1:
            legend_kwds={   
                'title': 'Fraction of Area Flooded (%)',     
                'ncol': 3,#len(flood_thresh)-1,
                'bbox_to_anchor': (1.2, 0.0), # Fine-tune the position relative to the plot
            }
        else:
            legend_kwds=None
        mapD.plot(column=col, ax=ax, legend=i==1, cmap='Blues', missing_kwds={"color": "lightgrey"},
                  scheme="UserDefined", classification_kwds={"bins": flood_thresh}, legend_kwds=legend_kwds)
        #ax.set_axis_off()
        ax.set_title(f'{flood_titles[i]} Flooding - {return_period}-Year Return Period')
        ax.set_facecolor('darkslategray')
        i += 1
    plt.tight_layout()
    plt.savefig(out_file)
    plt.close()

def calculate_think_hazard_score(inD, raster_path, depth_threshold, idx_col):
    """
    Calculate hazard score for a single administrative unit based on mean depth and area percentage.

    Args:
        inD: GeoDataFrame with geometry of the admin units
        raster_path: path to raster file (can be VRT or regular GeoTIFF)
        depth_threshold: minimum depth threshold for hazard scoring
    """
    with rasterio.Env(GDAL_HTTP_UNSAFESSL='YES'):
        curRaster = rasterio.open(raster_path)
        fCount = 0
        res = {}
        for idx, row in inD.iterrows():
            geometry = row["geometry"]
            fCount = fCount + 1
            ul = curRaster.index(*geometry.bounds[0:2])
            lr = curRaster.index(*geometry.bounds[2:4])
            # read the subset of the data into a numpy array
            window = (
                (float(lr[0]), float(ul[0] + 1)),
                (float(ul[1]), float(lr[1] + 1)),
            )
            try:
                data = curRaster.read(1, window=window)
                # create an affine transform for the subset data
                t = curRaster.transform
                shifted_affine = Affine(
                    t.a, t.b, t.c + ul[1] * t.a, t.d, t.e, t.f + lr[0] * t.e
                )

                # rasterize the geometry
                mask = rasterize(
                    [(geometry, 0)],
                    out_shape=data.shape,
                    transform=shifted_affine,
                    fill=1,
                    all_touched=False,
                    dtype=np.uint8,
                )

                # create a masked numpy array
                masked_data = np.ma.array(data=data, mask=mask.astype(bool))

                # calculate mean of values above threshold
                mean_val = masked_data[masked_data > depth_threshold].mean()
                # calculate area percentage above threshold
                area_flooded = (masked_data > depth_threshold).sum()

                res[idx] = {
                    idx_col: row[idx_col],
                    'frac_area_flooded': (area_flooded / masked_data.count()) * 100 if masked_data.count() > 0 else 0, 
                    
                    #"mean_val": mean_val,
                    #"area_flooded": area_flooded,
                    #"total_area": masked_data.count(),
                    
                }
            except Exception as e:
                #print(f"Error processing geometry at index {idx}: {e}")
                res[idx] = {
                    idx_col: row[idx_col],
                    'frac_area_flooded': 0, 
                    #"mean_val": 0,
                    #"area_flooded": 0,
                    #"total_area": 0,
                }
        return(pd.DataFrame.from_dict(res, orient='index'))





def calculate_mean_above_threshold(values, threshold=0):
    """
    Calculate mean of values greater than threshold.
    Returns 0 if no values meet the condition.
    """
    if values is None or len(values) == 0:
        return 0

    filtered_values = [v for v in values if v is not None and v > threshold]

    if len(filtered_values) == 0:
        return 0

    return np.mean(filtered_values)

def calculate_hazard_score(value_threshold, area_threshold_pct, *rp_stats):
    """
    Calculate hazard score based on value and area thresholds.

    Logic:
    - For each return period, check if BOTH value threshold and area threshold are met
    - Score = count of RPs meeting both thresholds (0, 1, 2, or 3)

    Args:
        value_threshold: Minimum mean value threshold
        area_threshold_pct: Minimum area percentage threshold
        *rp_stats: Tuples of (mean_value, area_pct) for each return period

    Returns:
        int: Hazard score (0-3)
    """
    score = 0

    for mean_val, area_pct in rp_stats:
        if mean_val >= value_threshold and area_pct >= area_threshold_pct:
            score += 1

    return score

def create_global_outputs(all_countries_results, output_dir, adm_level, period, scenario, flood_types):
    """
    Create global GeoPackage and summary Excel report from all country results.

    Args:
        all_countries_results: Dictionary mapping country codes to their results
        output_dir: Output directory
        adm_level: Administrative level
        period: Time period
        scenario: Climate scenario
        flood_types: List of flood type tuples
    """
    adm_level_str = f"ADM{adm_level}" if adm_level is not None else "ADM"

    if period == '2020':
        base_name = f"GLOBAL_FL_hazard_{adm_level_str}_{period}"
    else:
        base_name = f"GLOBAL_FL_hazard_{adm_level_str}_{period}_{scenario.split('-')[0]}"

    global_gpkg = os.path.join(output_dir, f"{base_name}.gpkg")
    global_excel = os.path.join(output_dir, f"{base_name}_summary.xlsx")

    # ========================================================================
    # 1. CREATE GLOBAL GEOPACKAGE
    # ========================================================================
    logger.info(f"Creating global GeoPackage: {global_gpkg}")

    # Process each flood type
    for ftype_short, ftype_long in flood_types:
        # Combine all countries for this flood type
        country_gdfs = []

        for country, results in all_countries_results.items():
            if ftype_short in results:
                gdf = results[ftype_short].copy()
                # Ensure ISO_A3 column exists
                if 'ISO_A3' not in gdf.columns:
                    gdf['ISO_A3'] = country
                country_gdfs.append(gdf)

        if country_gdfs:
            # Concatenate all countries
            global_gdf = pd.concat(country_gdfs, ignore_index=True)

            # Save to global geopackage
            layer = f"{ftype_short}_{period}"
            if period != '2020':
                layer = f"{ftype_short}_{period}_{scenario.split('-')[0]}"

            global_gdf.to_file(global_gpkg, layer=layer, driver="GPKG")
            logger.info(f"  Saved layer: {layer} ({len(global_gdf)} units from {len(country_gdfs)} countries)")

    # ========================================================================
    # 2. CREATE GLOBAL SUMMARY EXCEL
    # ========================================================================
    logger.info(f"Creating global summary Excel: {global_excel}")

    with pd.ExcelWriter(global_excel, engine='openpyxl') as writer:

        # Process each flood type
        for ftype_short, ftype_long in flood_types:

            # Collect country-level statistics
            country_stats = []

            for country in sorted(all_countries_results.keys()):
                results = all_countries_results[country]

                if ftype_short not in results:
                    continue

                gdf = results[ftype_short]

                # Calculate total area (in km²)
                total_area_m2 = gdf['unit_area_m2'].sum()
                total_area_km2 = total_area_m2 / 1_000_000

                # Count units by score
                score_counts = gdf['Hazard_score'].value_counts().sort_index()
                total_units = len(gdf)

                # Calculate area by score
                score_areas = {}
                for score in range(4):
                    score_units = gdf[gdf['Hazard_score'] == score]
                    score_area_m2 = score_units['unit_area_m2'].sum()
                    score_areas[score] = score_area_m2 / 1_000_000  # Convert to km²

                # Build statistics row
                stat_row = {
                    'Country': country,
                    'Total_Units': total_units,
                    'Total_Area_km2': round(total_area_km2, 2)
                }

                # Add count and percentage for each score
                for score in range(4):
                    count = score_counts.get(score, 0)
                    pct = (count / total_units * 100) if total_units > 0 else 0
                    stat_row[f'Score_{score}_Count'] = count
                    stat_row[f'Score_{score}_Count_Pct'] = round(pct, 2)

                # Add area and percentage for each score
                for score in range(4):
                    area = score_areas.get(score, 0)
                    pct = (area / total_area_km2 * 100) if total_area_km2 > 0 else 0
                    stat_row[f'Score_{score}_Area_km2'] = round(area, 2)
                    stat_row[f'Score_{score}_Area_Pct'] = round(pct, 2)

                country_stats.append(stat_row)

            if country_stats:
                # Create DataFrame
                df_stats = pd.DataFrame(country_stats)

                # Add global totals row
                global_totals = {
                    'Country': 'GLOBAL TOTAL',
                    'Total_Units': df_stats['Total_Units'].sum(),
                    'Total_Area_km2': round(df_stats['Total_Area_km2'].sum(), 2)
                }

                # Calculate global totals for each score
                for score in range(4):
                    global_totals[f'Score_{score}_Count'] = df_stats[f'Score_{score}_Count'].sum()
                    global_totals[f'Score_{score}_Area_km2'] = round(df_stats[f'Score_{score}_Area_km2'].sum(), 2)

                    # Calculate global percentages
                    total_units = global_totals['Total_Units']
                    total_area = global_totals['Total_Area_km2']

                    global_totals[f'Score_{score}_Count_Pct'] = round(
                        (global_totals[f'Score_{score}_Count'] / total_units * 100) if total_units > 0 else 0, 2
                    )
                    global_totals[f'Score_{score}_Area_Pct'] = round(
                        (global_totals[f'Score_{score}_Area_km2'] / total_area * 100) if total_area > 0 else 0, 2
                    )

                # Append global totals
                df_stats = pd.concat([df_stats, pd.DataFrame([global_totals])], ignore_index=True)

                # Save to Excel
                sheet_name = f"{ftype_short}_{period}"[:31]
                df_stats.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info(f"  Created summary sheet: {sheet_name}")

    logger.info(f"\nGlobal outputs created successfully!")
    logger.info(f"  GeoPackage: {global_gpkg}")
    logger.info(f"  Summary: {global_excel}")

def calculate_zonal_stats_for_unit(geometry, raster_path, value_threshold):
    """
    Calculate zonal statistics for a single administrative unit.
    Uses windowed reading - only reads the pixels needed for this geometry's bbox.
    Works efficiently with VRT files (only accesses relevant tiles).

    :param geometry: shapely geometry of the admin unit
    :param raster_path: path to raster file (can be VRT or regular GeoTIFF)
    :param value_threshold: minimum value threshold
    :return: tuple of (mean_above_zero, area_percentage)
    """
    try:
        with rst.open(raster_path) as src:
            # Get the geometry bounds
            geom_bounds = geometry.bounds  # (minx, miny, maxx, maxy)

            # Mask the raster with the geometry
            # crop=True limits reading to the geometry's bounding box
            # With VRT, this only reads the necessary tiles from disk
            out_image, out_transform = rio_mask(
                src,
                [geometry],
                crop=True,
                all_touched=True,
                nodata=-9999,
                filled=True  # Fill masked values with nodata
            )

            # Get the data array (first band)
            data = out_image[0]

            # Create mask for valid data
            # Handle multiple possible nodata values
            valid_mask = (data != -9999) & ~np.isnan(data)
            if src.nodata is not None:
                valid_mask &= (data != src.nodata)

            if not valid_mask.any():
                return 0, 0

            values = data[valid_mask]

            if len(values) == 0:
                return 0, 0

            # Calculate mean of values > 0
            mean_val = calculate_mean_above_threshold(values, threshold=0)

            # Count pixels above value threshold
            affected_pixels = np.sum(values > value_threshold)
            total_pixels = len(values)

            # Calculate area percentage
            area_pct = (affected_pixels / total_pixels) * 100 if total_pixels > 0 else 0

            return mean_val, area_pct

    except Exception as e:
        logger.error(f"Error calculating zonal stats: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0, 0
