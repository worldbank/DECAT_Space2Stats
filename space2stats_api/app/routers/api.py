from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator, ValidationInfo
from typing import List, Dict, Any, Literal
import geojson
from shapely.geometry import shape

router = APIRouter()

class AOI(BaseModel):
    type: Literal["Polygon"]
    coordinates: List[List[List[float]]]

    @field_validator("type")
    def validate_type(cls, v, info: ValidationInfo):
        if v != "Polygon":
            raise ValueError("aoi must be a polygon")
        return v

    @field_validator("coordinates")
    def validate_coordinates(cls, v, info: ValidationInfo):
        try:
            # create a shapely shape to validate the geometry
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
        # validate aoi as a valid shapely geometry
        geom = shape({"type": "Polygon", "coordinates": request.aoi.coordinates})
        if not geom.is_valid:
            raise ValueError("invalid geojson polygon")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
    # mock data retrieval process
    mock_data = [
        {"hex_id": "hex_1", "field1": "value1", "field2": "value2"},
        {"hex_id": "hex_2", "field1": "value3", "field2": "value4"},
        {"hex_id": "hex_3", "field1": "value5", "field2": "value6"}
    ]
    
    # filter data based on requested fields
    summaries = []
    for item in mock_data:
        summary = {"hex_id": item["hex_id"], "fields": {field: item[field] for field in request.fields if field in item}}
        summaries.append(summary)
    
    return summaries