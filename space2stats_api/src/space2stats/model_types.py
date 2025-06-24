from typing import Dict, Union

from geojson_pydantic import Feature, MultiPolygon, Polygon
from typing_extensions import TypeAlias

AoiModel: TypeAlias = Feature[Union[Polygon, MultiPolygon], Dict]
