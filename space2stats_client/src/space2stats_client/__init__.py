"""Space2Stats-Client - A World Bank Python client for accessing spatial statistics."""

from .client import Space2StatsClient

__version__ = "1.3.2"
__license__ = "World Bank Master Community License Agreement"
__copyright__ = "Copyright (c) 2025 The World Bank"

# Widgets are available as optional components
# Install with: pip install space2stats-client[widgets]
# Then import like:
#   from space2stats_client.widgets import CrossSectionFieldSelector, TimeSeriesFieldSelector, AOISelector
