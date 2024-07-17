import os
import psycopg2
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import List, Dict, Any, Literal
from shapely.geometry import shape, mapping
from dotenv import load_dotenv
import s3fs
from app.utils.h3_utils import generate_h3_ids

load_dotenv("../db.env")
s3 = s3fs.S3FileSystem()

# Load PostgreSQL connection details from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

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
    FROM space2stats_nyc_sample
    WHERE hex_id IN ({h3_ids_str})
    """

    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(sql_query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not rows:
        return []

    summaries = []
    for row in rows:
        summary = {"hex_id": row[0], "fields": {col: row[idx] for idx, col in enumerate(colnames[1:], start=1) if col in request.fields}}
        summaries.append(summary)

    return summaries