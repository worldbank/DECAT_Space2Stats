from typing import List, Literal, Optional

from geojson_pydantic import Feature
from pydantic import BaseModel

from ..types import AoiModel


class SummaryRequest(BaseModel):
    aoi: AoiModel
    spatial_join_method: Literal["touches", "centroid", "within"]
    fields: List[str]
    geometry: Optional[Literal["polygon", "point"]] = None


class HexIdSummaryRequest(BaseModel):
    hex_ids: List[str]
    fields: List[str]
    geometry: Optional[Literal["polygon", "point"]] = None


class AggregateRequest(BaseModel):
    aoi: Feature
    spatial_join_method: Literal["touches", "centroid", "within"]
    fields: List[str]
    aggregation_type: Literal["sum", "avg", "count", "max", "min"]


class HexIdAggregateRequest(BaseModel):
    hex_ids: List[str]
    fields: List[str]
    aggregation_type: Literal["sum", "avg", "count", "max", "min"]


class TimeseriesRequest(BaseModel):
    aoi: Feature
    spatial_join_method: Literal["touches", "centroid", "within"]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    fields: Optional[List[str]] = None


class HexIdTimeseriesRequest(BaseModel):
    hex_ids: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    fields: Optional[List[str]] = None
