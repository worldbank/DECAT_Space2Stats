"""Optional widgets for Space2Stats client.

Widgets provide interactive UI components for the Space2Stats client.
Install with: pip install space2stats-client[widgets]

Example:
    from space2stats_client.widgets.cross_section_field_selector import CrossSectionFieldSelector, TimeSeriesFieldSelector, AOISelector
"""

__all__ = []

# Import widgets if dependencies are available
try:
    from .cross_section_field_selector import CrossSectionFieldSelector

    __all__.append("CrossSectionFieldSelector")
except ImportError:
    pass

try:
    from .time_series_field_selector import TimeSeriesFieldSelector

    __all__.append("TimeSeriesFieldSelector")
except ImportError:
    pass

try:
    from .aoi_selector import AOISelector

    __all__.append("AOISelector")
except ImportError:
    pass
