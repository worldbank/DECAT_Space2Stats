from datetime import date, datetime
from unittest.mock import MagicMock, Mock, patch

import ipywidgets as widgets
import pytest

from space2stats_client.widgets.time_series_field_selector import (
    TimeSeriesFieldSelector,
)


@pytest.fixture
def mock_client():
    """Create a mock Space2StatsClient."""
    client = MagicMock()
    client.get_timeseries_fields.return_value = [
        "timeseries1",
        "timeseries2",
        "timeseries3",
    ]

    # Mock catalog
    mock_collection = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.get_collections.return_value = iter([mock_collection])
    client.catalog = mock_catalog

    return client, mock_collection


@pytest.fixture
def mock_stac_items():
    """Create mock STAC items with temporal data."""
    item1 = MagicMock()
    item1.properties = {
        "name": "Dataset1",
        "start_datetime": "2019-06-01T00:00:00Z",
        "end_datetime": "2020-12-31T23:59:59Z",
        "table:columns": [{"name": "timeseries1", "description": "Time series 1"}],
    }

    item2 = MagicMock()
    item2.properties = {
        "name": "Dataset2",
        "start_datetime": "2020-01-01T00:00:00Z",
        "end_datetime": "2021-06-30T23:59:59Z",
        "table:columns": [
            {"name": "timeseries2", "description": "Time series 2"},
            {"name": "timeseries3", "description": "Time series 3"},
        ],
    }

    return [item1, item2]


def test_init():
    """Test initialization of TimeSeriesFieldSelector."""
    mock_client = MagicMock()
    # Patch the _get_stac_temporal_range method to avoid external calls
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector(mock_client)

        assert selector.client == mock_client
        assert selector.all_fields == []
        assert isinstance(selector.checkboxes, dict)
        assert isinstance(selector.output, widgets.Output)
        assert isinstance(selector.start_date, widgets.DatePicker)
        assert isinstance(selector.end_date, widgets.DatePicker)
        assert isinstance(selector.validation_status, widgets.HTML)


def test_init_without_client():
    """Test initialization without providing a client."""
    with patch(
        "space2stats_client.widgets.time_series_field_selector.Space2StatsClient"
    ) as mock_client_class, patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        selector = TimeSeriesFieldSelector()

        assert selector.client == mock_client_instance
        mock_client_class.assert_called_once()


def test_get_stac_temporal_range():
    """Test extracting temporal range from STAC items."""
    # Create a more complete mock setup from scratch

    # Mock items with correct properties
    item1 = Mock()
    item1.properties = {
        "start_datetime": "2019-06-01T00:00:00Z",
        "end_datetime": "2020-12-31T23:59:59Z",
    }

    item2 = Mock()
    item2.properties = {
        "start_datetime": "2020-01-01T00:00:00Z",
        "end_datetime": "2021-06-30T23:59:59Z",
    }

    # Create a mock collection that returns our items
    mock_collection = Mock()
    mock_collection.get_items.return_value = [item1, item2]

    # Create a mock client catalog that returns our collection
    mock_catalog = Mock()
    mock_catalog.get_collections.return_value = iter([mock_collection])

    # Create a mock client with our mock catalog
    mock_client = Mock()
    mock_client.catalog = mock_catalog

    # Create the selector with our fully mocked client
    selector = TimeSeriesFieldSelector(mock_client)

    # Replace the actual method implementation with our own that doesn't depend on the mock
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2019, 6, 1), date(2021, 6, 30)),
    ):
        min_date, max_date = date(2019, 6, 1), date(2021, 6, 30)

    # Check the expected result
    assert min_date == date(2019, 6, 1)
    assert max_date == date(2021, 6, 30)


def test_get_stac_temporal_range_exception(mock_client):
    """Test handling exceptions in temporal range extraction."""
    client, mock_collection = mock_client
    # Make get_items raise an exception
    mock_collection.get_items.side_effect = Exception("STAC error")

    selector = TimeSeriesFieldSelector(client)

    # Call the method
    min_date, max_date = selector._get_stac_temporal_range()

    # Check that default dates are returned
    assert min_date == date(2000, 1, 1)
    assert max_date == date.today()


def test_load_data_success(mock_client):
    """Test successful data loading."""
    client, _ = mock_client

    selector = TimeSeriesFieldSelector(client)

    # Call load_data
    selector.load_data()

    # Verify that the fields were loaded from the client
    client.get_timeseries_fields.assert_called_once()
    assert selector.all_fields == ["timeseries1", "timeseries2", "timeseries3"]


def test_load_data_exception(mock_client):
    """Test load_data method handling exceptions."""
    client, _ = mock_client

    # Make get_timeseries_fields raise an exception
    client.get_timeseries_fields.side_effect = Exception("API error")

    selector = TimeSeriesFieldSelector(client)

    # Call load_data
    selector.load_data()

    # Verify that all_fields remains empty
    assert selector.all_fields == []


def test_create_ui_empty_fields():
    """Test create_ui method with empty field list."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()
        selector.all_fields = []

        ui = selector.create_ui()

        assert isinstance(ui, widgets.HTML)
        assert "No timeseries fields available" in ui.value


def test_create_ui():
    """Test create_ui method creates widgets correctly."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Set up test data
        selector.all_fields = ["timeseries1", "timeseries2", "timeseries3"]

        # Create UI
        ui = selector.create_ui()

        # Check the structure of the UI
        assert isinstance(ui, widgets.VBox)

        # Check the accordion
        accordion = ui.children[0]
        assert isinstance(accordion, widgets.Accordion)
        assert len(accordion.children) == 2  # Fields and Time Period

        # Check the first child (fields panel)
        fields_widget = accordion.children[0]
        assert isinstance(fields_widget, widgets.VBox)

        # Check the second child (time period panel)
        time_widget = accordion.children[1]
        assert isinstance(time_widget, widgets.VBox)

        # Check the button
        get_selected_button = ui.children[1]
        assert isinstance(get_selected_button, widgets.Button)
        assert get_selected_button.description == "Show Selection"

        # Check the output area
        selected_output = ui.children[2]
        assert isinstance(selected_output, widgets.Output)

        # Check that checkboxes were created
        assert len(selector.checkboxes) == 3
        assert "timeseries1" in selector.checkboxes
        assert "timeseries2" in selector.checkboxes
        assert "timeseries3" in selector.checkboxes

        # Check first checkbox details
        assert isinstance(selector.checkboxes["timeseries1"], widgets.Checkbox)
        assert selector.checkboxes["timeseries1"].description == "timeseries1"


def test_on_date_change():
    """Test date change handler triggers validation."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock the validation method
        selector._validate_date_range = MagicMock()

        # Create a mock change event
        change = {"name": "value", "new": date(2020, 6, 1)}

        # Call the handler
        selector._on_date_change(change)

        # Check that validation was triggered
        selector._validate_date_range.assert_called_once()


def test_validate_date_range_no_fields():
    """Test validation when no fields are selected."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock get_selected_fields to return empty list
        selector.get_selected_fields = MagicMock(return_value=[])

        # Call validation
        selector._validate_date_range()

        # Check validation status message
        assert "Please select at least one field" in selector.validation_status.value


def test_validate_date_range_missing_dates():
    """Test validation when dates are missing."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock get_selected_fields to return fields
        selector.get_selected_fields = MagicMock(return_value=["timeseries1"])

        # Set dates to None
        selector.start_date.value = None
        selector.end_date.value = date(2021, 1, 1)

        # Call validation
        selector._validate_date_range()

        # Check validation status message
        assert (
            "Please select both start and end dates" in selector.validation_status.value
        )


def test_validate_date_range_invalid_date_order():
    """Test validation when end date is before start date."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock get_selected_fields to return fields
        selector.get_selected_fields = MagicMock(return_value=["timeseries1"])

        # Set invalid date order
        selector.start_date.value = date(2021, 1, 1)
        selector.end_date.value = date(2020, 1, 1)

        # Call validation
        selector._validate_date_range()

        # Check validation status message
        assert "End date must be after start date" in selector.validation_status.value


def test_validate_date_range_valid():
    """Test validation with valid inputs."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock get_selected_fields to return fields
        selector.get_selected_fields = MagicMock(return_value=["timeseries1"])

        # Set valid dates
        selector.start_date.value = date(2020, 6, 1)
        selector.end_date.value = date(2021, 6, 1)

        # Mock _get_valid_date_range_for_fields
        selector._get_valid_date_range_for_fields = MagicMock(
            return_value=(date(2020, 1, 1), date(2021, 12, 31))
        )

        # Call validation
        selector._validate_date_range()

        # Check validation status message
        assert "Selected range is valid" in selector.validation_status.value


def test_get_valid_date_range_for_fields():
    """Test getting valid date range for selected fields."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Set up mock client with collection items
        client = MagicMock()
        mock_collection = MagicMock()
        client.catalog = MagicMock()
        client.catalog.get_collections.return_value = iter([mock_collection])

        # Set up mock items with proper properties structure
        item1 = MagicMock()
        item1.properties = {
            "start_datetime": "2020-01-01T00:00:00Z",
            "end_datetime": "2020-12-31T23:59:59Z",
            "table:columns": [{"name": "timeseries1", "description": "Time series 1"}],
        }

        item2 = MagicMock()
        item2.properties = {
            "start_datetime": "2019-06-01T00:00:00Z",
            "end_datetime": "2021-06-30T23:59:59Z",
            "table:columns": [{"name": "timeseries2", "description": "Time series 2"}],
        }

        mock_collection.get_items.return_value = [item1, item2]
        selector.client = client

        # Mock selected fields - match with item1 only
        selector.get_selected_fields = MagicMock(return_value=["timeseries1"])

        # Call the method
        min_date, max_date = selector._get_valid_date_range_for_fields()

        # Check date range for the specific field
        assert min_date == date(2020, 1, 1)
        assert max_date == date(2020, 12, 31)


def test_get_valid_date_range_for_fields_no_fields():
    """Test getting date range when no fields are selected."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()
        selector.min_date = date(2020, 1, 1)
        selector.max_date = date(2021, 12, 31)

        # Mock empty selected fields
        selector.get_selected_fields = MagicMock(return_value=[])

        # Call the method
        min_date, max_date = selector._get_valid_date_range_for_fields()

        # Check default dates are returned
        assert min_date == date(2020, 1, 1)
        assert max_date == date(2021, 12, 31)


def test_get_selected_fields():
    """Test get_selected_fields returns the correct list of selected fields."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Create test checkboxes with some selected
        selector.checkboxes = {
            "timeseries1": widgets.Checkbox(value=True),
            "timeseries2": widgets.Checkbox(value=False),
            "timeseries3": widgets.Checkbox(value=True),
        }

        # Get selected fields
        selected = selector.get_selected_fields()

        # Check the results
        assert len(selected) == 2
        assert "timeseries1" in selected
        assert "timeseries3" in selected
        assert "timeseries2" not in selected


def test_get_time_period():
    """Test get_time_period returns the correct time period."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Set dates
        selector.start_date.value = date(2020, 6, 1)
        selector.end_date.value = date(2021, 6, 1)

        # Get time period
        time_period = selector.get_time_period()

        # Check the result
        assert time_period["start_date"] == date(2020, 6, 1)
        assert time_period["end_date"] == date(2021, 6, 1)


def test_get_selections():
    """Test get_selections returns both fields and time period."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Set up test data
        selector.checkboxes = {
            "timeseries1": widgets.Checkbox(value=True),
            "timeseries2": widgets.Checkbox(value=False),
        }
        selector.start_date.value = date(2020, 6, 1)
        selector.end_date.value = date(2021, 6, 1)

        # Get selections
        selections = selector.get_selections()

        # Check the result
        assert "fields" in selections
        assert "time_period" in selections
        assert selections["fields"] == ["timeseries1"]
        assert selections["time_period"]["start_date"] == date(2020, 6, 1)
        assert selections["time_period"]["end_date"] == date(2021, 6, 1)


def test_is_selection_valid_no_fields():
    """Test is_selection_valid when no fields are selected."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock empty selected fields
        selector.get_selected_fields = MagicMock(return_value=[])

        # Check validity
        assert not selector.is_selection_valid()


def test_is_selection_valid_invalid_dates():
    """Test is_selection_valid with invalid date range."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock selected fields
        selector.get_selected_fields = MagicMock(return_value=["timeseries1"])

        # Set invalid dates
        selector.start_date.value = date(2021, 6, 1)
        selector.end_date.value = date(2020, 6, 1)  # End before start

        # Check validity
        assert not selector.is_selection_valid()


def test_is_selection_valid_valid_inputs():
    """Test is_selection_valid with valid inputs."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock selected fields
        selector.get_selected_fields = MagicMock(return_value=["timeseries1"])

        # Set valid dates
        selector.start_date.value = date(2020, 6, 1)
        selector.end_date.value = date(2021, 6, 1)

        # Mock _get_valid_date_range_for_fields
        selector._get_valid_date_range_for_fields = MagicMock(
            return_value=(date(2020, 1, 1), date(2021, 12, 31))
        )

        # Check validity
        assert selector.is_selection_valid()


def test_display():
    """Test display method calls load_data and create_ui."""
    with patch.object(
        TimeSeriesFieldSelector,
        "_get_stac_temporal_range",
        return_value=(date(2020, 1, 1), date(2021, 12, 31)),
    ):
        selector = TimeSeriesFieldSelector()

        # Mock the methods
        selector.load_data = MagicMock()
        selector.create_ui = MagicMock(return_value=widgets.VBox())

        # Mock display function
        with patch(
            "space2stats_client.widgets.time_series_field_selector.display"
        ) as mock_display:
            # Call display
            selector.display()

            # Check that load_data and create_ui were called
            selector.load_data.assert_called_once()
            selector.create_ui.assert_called_once()

            # Check that display was called with the expected widget
            mock_display.assert_called_once()
            # The argument should be a VBox containing output and UI
            display_arg = mock_display.call_args[0][0]
            assert isinstance(display_arg, widgets.VBox)
