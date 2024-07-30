from typing import List, Dict, Any, Literal, Optional
from fastapi import APIRouter

from pydantic import BaseModel, create_model
from geojson_pydantic import Feature, Polygon

from app.utils.h3_utils import generate_h3_ids, generate_h3_geometries
from app.utils.db_utils import get_available_fields, get_summaries


router = APIRouter()

AOIModel = Feature[Polygon, Dict]


class SummaryRequest(BaseModel):
    aoi: AOIModel
    spatial_join_method: Literal["touches", "centroid", "within"]
    fields: List[str]
    geometry: Optional[Literal["polygon", "point"]] = False


def create_response_model(fields: List[str]):
    field_definitions = {field: (Any, ...) for field in fields}
    field_definitions["hex_id"] = (str, ...)
    if "geometry" in fields:
        field_definitions["geometry"] = (Dict[str, Any], ...)
    return create_model("DynamicSummaryResponse", **field_definitions)


@router.post("/summary", response_model=List[Dict[str, Any]])
def get_summary(request: SummaryRequest):
    resolution = 6
    h3_ids = generate_h3_ids(
        dict(request.aoi.geometry), resolution, request.spatial_join_method
    )

    if not h3_ids:
        return []

    rows, colnames = get_summaries(request.fields, h3_ids)
    if not rows:
        return []

    summaries = []
    geometries = (
        generate_h3_geometries(h3_ids, request.geometry) if request.geometry else None
    )

    for idx, row in enumerate(rows):
        summary = {"hex_id": row[0]}
        if request.geometry and geometries:
            summary["geometry"] = geometries[idx]
        summary.update(
            {
                col: row[idx]
                for idx, col in enumerate(colnames[1:], start=1)
                if col in request.fields
            }
        )
        summaries.append(summary)

    return summaries


@router.get("/fields", response_model=List[str])
def fields():
    return get_available_fields()
