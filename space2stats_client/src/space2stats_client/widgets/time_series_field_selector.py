from datetime import date, datetime
from typing import Dict, List, Optional

import ipywidgets as widgets
from IPython.display import display

from ..client import Space2StatsClient


class TimeSeriesFieldSelector:
    """Interactive field selector for Space2Stats API timeseries data.

    This widget allows users to interactively select fields from the Space2Stats API
    timeseries endpoint, and specify a time period for the data.
    """

    def __init__(self, client=None):
        """Initialize the timeseries field selector.

        Args:
            client: An instance of Space2StatsClient. If not provided, a new one will be created.
        """
        self.client = client or Space2StatsClient()
        self.all_fields = []
        self.checkboxes = {}
        self.output = widgets.Output()

        # Time period widgets
        self.time_period_label = widgets.HTML("<h3>Select Time Period</h3>")

        # Get temporal extent from STAC catalog
        self.min_date, self.max_date = self._get_stac_temporal_range()

        # Create date picker widgets with dynamic range
        self.start_date = widgets.DatePicker(
            description="Start Date:", disabled=False, value=self.min_date
        )

        self.end_date = widgets.DatePicker(
            description="End Date:", disabled=False, value=self.max_date
        )

        # Create validation status widget
        self.validation_status = widgets.HTML(
            value="<p style='color: orange;'>Please select at least one field to validate date range</p>"
        )

        # Create the time selection layout
        self.time_selection_box = widgets.VBox(
            [
                self.time_period_label,
                self.start_date,
                self.end_date,
                self.validation_status,
            ]
        )

        # Register observers for date changes
        self.start_date.observe(self._on_date_change, names="value")
        self.end_date.observe(self._on_date_change, names="value")

    def _get_stac_temporal_range(self):
        """Extract temporal range from STAC catalog items."""
        try:
            # Get the collection
            collection = next(self.client.catalog.get_collections())
            items = list(collection.get_items())

            # Get min and max dates from all items
            min_date = None
            max_date = None

            for item in items:
                if "start_datetime" in item.properties:
                    start_datetime = item.properties["start_datetime"]
                    # Handle ISO format with or without timezone
                    if "Z" in start_datetime:
                        start_datetime = start_datetime.replace("Z", "+00:00")
                    start_date = datetime.fromisoformat(start_datetime).date()
                    if min_date is None or start_date < min_date:
                        min_date = start_date

                if "end_datetime" in item.properties:
                    end_datetime = item.properties["end_datetime"]
                    # Handle ISO format with or without timezone
                    if "Z" in end_datetime:
                        end_datetime = end_datetime.replace("Z", "+00:00")
                    end_date = datetime.fromisoformat(end_datetime).date()
                    if max_date is None or end_date > max_date:
                        max_date = end_date

            # If no valid dates found, use defaults
            if min_date is None:
                min_date = date(2000, 1, 1)
            if max_date is None:
                max_date = date.today()

            return min_date, max_date

        except Exception as e:
            print(f"✗ Error extracting temporal range from STAC: {e}")
            print("Using default range: 2000-01-01 to today")
            return date(2000, 1, 1), date.today()

    def _on_date_change(self, change):
        """Validate date range when dates change."""
        if change["name"] == "value":
            self._validate_date_range()

    def load_data(self):
        """Load timeseries fields from API."""
        print("Loading timeseries fields from API...")

        # Load fields using the timeseries fields endpoint
        try:
            self.all_fields = self.client.get_timeseries_fields()
            print(f"✓ {len(self.all_fields)} timeseries fields loaded from API")
        except Exception as e:
            print(f"✗ Error loading timeseries fields: {e}")
            return

    def create_ui(self):
        """Create the UI for field selection."""
        if not self.all_fields:
            return widgets.HTML(
                "No timeseries fields available. Please check the output for errors."
            )

        # Create "Select All" checkbox
        select_all = widgets.Checkbox(
            value=False,
            description=f"Select All ({len(self.all_fields)})",
            indent=False,
            layout=widgets.Layout(width="300px"),
        )

        # Individual field checkboxes
        field_checkboxes = []
        for field in sorted(self.all_fields):
            checkbox = widgets.Checkbox(
                value=False,
                description=field,
                indent=False,
                layout=widgets.Layout(width="400px"),
            )
            field_checkboxes.append(checkbox)
            self.checkboxes[field] = checkbox

            def make_checkbox_handler(field_name):
                def handler(change):
                    if change["name"] == "value":
                        self._validate_date_range()

                return handler

            checkbox.observe(make_checkbox_handler(field), names="value")

        # Link "Select All" to toggle all checkboxes
        def select_all_handler(change):
            if change["type"] == "change" and change["name"] == "value":
                for checkbox in self.checkboxes.values():
                    checkbox.value = change["new"]
                self._validate_date_range()

        select_all.observe(select_all_handler, names="value")

        # Create fields widget box
        fields_widget = widgets.VBox([select_all] + field_checkboxes)

        # Create a button to get selected fields
        get_selected_button = widgets.Button(
            description="Get Selected Fields",
            button_style="primary",
            tooltip="Get the list of selected fields",
        )

        # Output area for selected fields
        selected_output = widgets.Output()

        def on_get_selected_clicked(b):
            selected = self.get_selected_fields()
            with selected_output:
                selected_output.clear_output()
                if selected:
                    print(f"Selected {len(selected)} fields:")
                    print(selected)
                else:
                    print("No fields selected")

        get_selected_button.on_click(on_get_selected_clicked)

        # Create accordion for fields and time period
        accordion = widgets.Accordion(children=[fields_widget, self.time_selection_box])
        accordion.set_title(0, f"Timeseries Fields ({len(self.all_fields)})")
        accordion.set_title(1, "Time Period")

        # Combine all widgets
        ui = widgets.VBox([accordion, get_selected_button, selected_output])

        return ui

    def _validate_date_range(self):
        """Check if the selected date range is valid for the selected fields."""
        selected_fields = self.get_selected_fields()
        if not selected_fields:
            self.validation_status.value = "<p style='color: orange;'>Please select at least one field to validate date range</p>"
            return

        # Get the current selected dates
        start_date = self.start_date.value
        end_date = self.end_date.value

        if start_date is None or end_date is None:
            self.validation_status.value = (
                "<p style='color: orange;'>Please select both start and end dates</p>"
            )
            return

        if end_date < start_date:
            self.validation_status.value = (
                "<p style='color: red;'>End date must be after start date</p>"
            )
            return

        try:
            # Get the valid date range for selected fields
            valid_min_date, valid_max_date = self._get_valid_date_range_for_fields()

            # Check if selected range is within valid range
            if start_date >= valid_min_date and end_date <= valid_max_date:
                self.validation_status.value = f"<p style='color: green;'>Selected range is valid. The valid range for the selected fields is {valid_min_date} to {valid_max_date}</p>"
            else:
                self.validation_status.value = f"<p style='color: red;'>Selected range is not valid. The valid range for the selected fields is {valid_min_date} to {valid_max_date}</p>"

        except Exception as e:
            self.validation_status.value = (
                f"<p style='color: red;'>Error validating date range: {e}</p>"
            )

    def _get_valid_date_range_for_fields(self):
        """Update the date range based on selected fields."""
        selected_fields = self.get_selected_fields()
        if not selected_fields:
            with self.output:
                print("Please select at least one field to update the date range.")
            return self.min_date, self.max_date

        try:
            # Get the collection
            collection = next(self.client.catalog.get_collections())
            items = list(collection.get_items())

            # Find items that contain any of the selected fields
            relevant_items = []
            for item in items:
                if "table:columns" in item.properties:
                    field_names = [
                        col.get("name", "").lower()
                        for col in item.properties["table:columns"]
                    ]
                    if any(field.lower() in field_names for field in selected_fields):
                        relevant_items.append(item)

            if not relevant_items:
                with self.output:
                    print("No STAC items found containing the selected fields.")
                return self.min_date, self.max_date

            # Get min and max dates from relevant items
            min_date = None
            max_date = None

            for item in relevant_items:
                if "start_datetime" in item.properties:
                    start_datetime = item.properties["start_datetime"]
                    if "Z" in start_datetime:
                        start_datetime = start_datetime.replace("Z", "+00:00")
                    start_date = datetime.fromisoformat(start_datetime).date()
                    if min_date is None or start_date < min_date:
                        min_date = start_date

                if "end_datetime" in item.properties:
                    end_datetime = item.properties["end_datetime"]
                    if "Z" in end_datetime:
                        end_datetime = end_datetime.replace("Z", "+00:00")
                    end_date = datetime.fromisoformat(end_datetime).date()
                    if max_date is None or end_date > max_date:
                        max_date = end_date

            # If no valid dates found, keep the current range
            if min_date is None:
                min_date = self.min_date
            if max_date is None:
                max_date = self.max_date

            return min_date, max_date

        except Exception as e:
            with self.output:
                print(f"✗ Error updating date range: {e}")
            return self.min_date, self.max_date

    def get_selected_fields(self):
        """Get the list of selected timeseries fields.

        Returns:
            A list of selected field names.
        """
        selected = []
        for field, checkbox in self.checkboxes.items():
            if checkbox.value:
                selected.append(field)
        return selected

    def get_time_period(self):
        """Get the selected time period information."""
        return {
            "start_date": self.start_date.value,
            "end_date": self.end_date.value,
        }

    def get_selections(self):
        """Get both field and time period selections."""
        return {
            "fields": self.get_selected_fields(),
            "time_period": self.get_time_period(),
        }

    def is_selection_valid(self):
        """Check if the current selection is valid."""
        selected_fields = self.get_selected_fields()
        if not selected_fields:
            return False

        start_date = self.start_date.value
        end_date = self.end_date.value

        if start_date is None or end_date is None or end_date < start_date:
            return False

        range_result = self._get_valid_date_range_for_fields()
        if not range_result:
            return False

        valid_min_date, valid_max_date = range_result
        return start_date >= valid_min_date and end_date <= valid_max_date

    def display(self):
        """Display the timeseries field selector UI."""
        self.load_data()
        ui = self.create_ui()
        display(widgets.VBox([self.output, ui]))
