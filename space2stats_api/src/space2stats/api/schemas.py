from typing import List, Literal, Optional

from pydantic import BaseModel

from ..types import AoiModel


class SummaryRequest(BaseModel):
    aoi: AoiModel
    spatial_join_method: Literal["touches", "centroid", "within"]
    fields: List[str]
    geometry: Optional[Literal["polygon", "point"]] = None
