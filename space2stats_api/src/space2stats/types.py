from typing import Dict

from geojson_pydantic import Feature, Polygon
from typing_extensions import TypeAlias

AoiModel: TypeAlias = Feature[Polygon, Dict]
