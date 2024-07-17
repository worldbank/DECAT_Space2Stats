from typing import List, Dict, Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from shapely.geometry import shape, mapping

from app.utils.h3_utils import generate_h3_ids
from app.utils.db_utils import get_available_fields, get_summaries


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

    try:
        rows, colnames = get_summaries(request.fields, h3_ids)
        if not rows:
            return []
    except Exception as e:
        HTTPException(status_code=500, detail=str(e))

    summaries = []
    for row in rows:
        summary = {"hex_id": row[0], "fields": {col: row[idx] for idx, col in enumerate(colnames[1:], start=1) if col in request.fields}}
        summaries.append(summary)

    return summaries

@router.get("/fields", response_model=List[str])
def fields():
    try:
        fields = get_available_fields()
        print(fields)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return fields
