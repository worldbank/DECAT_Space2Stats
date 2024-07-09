import duckdb
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import List, Dict, Any, Literal
from shapely.geometry import shape, mapping
from dotenv import load_dotenv
import s3fs
from app.utils.h3_utils import generate_h3_ids

load_dotenv("../wb_aws.env")
s3 = s3fs.S3FileSystem()

duckdb_file = '../combined_population.duckdb'
con = duckdb.connect(duckdb_file)

router = APIRouter()

class AOI(BaseModel):
    type: Literal["Polygon"]
    coordinates: List[List[List[float]]]

    @field_validator("type")
    def validate_type(cls, v):
        if v != "Polygon":
            raise ValueError("aoi must be a polygon")
        return v

    @field_validator("coordinates")
    def validate_coordinates(cls, v):
        try:
            geom = shape({"type": "Polygon", "coordinates": v})
            if not geom.is_valid:
                raise ValueError("invalid coordinates for polygon")
        except Exception as e:
            raise ValueError("invalid coordinates for polygon") from e
        return v

class SummaryRequest(BaseModel):
    aoi: AOI
    spatial_join_method: Literal["touches", "centroid", "within"]
    fields: List[str]

class SummaryResponse(BaseModel):
    hex_id: str
    fields: Dict[str, Any]

@router.post("/summary", response_model=List[SummaryResponse])
def get_summary(request: SummaryRequest):
    try:
        geom = shape({"type": "Polygon", "coordinates": request.aoi.coordinates})
        if not geom.is_valid:
            raise ValueError("invalid geojson polygon")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    resolution = 6
    aoi_geojson = mapping(geom)
    h3_ids = generate_h3_ids(aoi_geojson, resolution, request.spatial_join_method)

    if not h3_ids:
        return []

    h3_ids_str = ', '.join(f"'{h3_id}'" for h3_id in h3_ids)
    sql_query = f"""
    SELECT hex_id, {', '.join(request.fields)}
    FROM combined_population
    WHERE hex_id IN ({h3_ids_str})
    """

    result = con.execute(sql_query).fetchdf()

    if result.empty:
        return []

    summaries = []
    for _, row in result.iterrows():
        summary = {"hex_id": row["hex_id"], "fields": {field: row[field] for field in request.fields if field in row}}
        summaries.append(summary)

    return summaries