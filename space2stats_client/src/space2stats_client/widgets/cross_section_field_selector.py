"""Interactive widgets for Space2Stats client."""

from typing import Dict, List, Optional

import ipywidgets as widgets
from IPython.display import display

from ..client import Space2StatsClient


class CrossSectionFieldSelector:
    """Interactive field selector for Space2Stats API.

    This widget allows users to interactively select fields from the Space2Stats API,
    organized by their source STAC items.

    Example:
        ```python
        from space2stats_client import Space2StatsClient
        from space2stats_client.widgets import CrossSectionFieldSelector

        client = Space2StatsClient()
        selector = CrossSectionFieldSelector(client)
        selector.display()

        # Later, get selected fields
        selected_fields = selector.get_selected_fields()
        ```
    """

    def __init__(self, client: Optional[Space2StatsClient] = None):
        """Initialize the field selector.

        Args:
            client: An instance of Space2StatsClient. If not provided, a new one will be created.
        """
        self.client = client or Space2StatsClient()
        self.all_fields = []
        self.field_groups = {}
        self.checkboxes = {}
        self.output = widgets.Output()
        self.field_descriptions = {}
        self.field_to_group = {}

    def load_data(self):
        """Load data from API and STAC collection."""
        with self.output:
            print("Loading data from API and STAC collection...")

            # Load fields from API
            try:
                self.all_fields = self.client.get_fields()
                print(f"✓ {len(self.all_fields)} fields loaded from API")
            except Exception as e:
                print(f"✗ Error loading fields: {e}")
                return

            # Group fields by STAC items
            self._group_fields_by_stac_items()

    def _group_fields_by_stac_items(self):
        """Group fields by their source STAC items."""
        with self.output:
            try:
                # Access the catalog and get collection items directly
                collection = next(self.client.catalog.get_collections())
                items = list(collection.get_items())

                # Initialize field groups dictionary
                self.field_groups = {}
                self.field_descriptions = {}
                self.field_to_group = {}

                # Process each item
                for item in items:
                    item_data = item.to_dict()

                    # Skip this item if it has start_datetime
                    if item_data.get("properties", {}).get("start_datetime"):
                        continue

                    # Use the item's title or name as the group name
                    group_name = item_data.get("title") or item_data.get(
                        "properties", {}
                    ).get("name", item_data.get("id"))

                    # Get the table columns from the item properties
                    if "table:columns" in item_data.get("properties", {}):
                        columns = item_data["properties"]["table:columns"]

                        # Extract field names and descriptions
                        fields_in_item = []
                        for column in columns:
                            field_name = column.get("name")

                            if field_name and field_name.lower() in [
                                f.lower() for f in self.all_fields
                            ]:
                                # Use the case from the STAC item
                                fields_in_item.append(field_name)
                                # Store description for tooltip
                                self.field_descriptions[field_name] = column.get(
                                    "description", ""
                                )
                                # Map field to group
                                self.field_to_group[field_name] = group_name

                        if fields_in_item:
                            self.field_groups[group_name] = fields_in_item

                # Add an "Uncategorized" group for fields not found in any STAC item
                categorized_fields = [
                    field for group in self.field_groups.values() for field in group
                ]
                uncategorized_fields = [
                    field
                    for field in self.all_fields
                    if field not in categorized_fields
                ]

                if uncategorized_fields:
                    self.field_groups["Uncategorized"] = uncategorized_fields

            except Exception as e:
                print(f"Error grouping fields by STAC items: {e}")
                import traceback

                traceback.print_exc()

                # If STAC processing fails, put all fields in one group
                self.field_groups = {"All Fields": self.all_fields}
                print("✓ Fallback: All fields added to a single group")

    def create_ui(self):
        """Create the UI for field selection."""
        if not self.field_groups:
            return widgets.HTML(
                "No field groups available. Please check the output for errors."
            )

        # Create accordion for field groups
        accordion = widgets.Accordion()
        children = []

        # Create checkboxes for each group
        for group, fields in self.field_groups.items():
            # Create a VBox of checkboxes for this group
            group_checkboxes = []

            # Add "Select All" checkbox
            select_all = widgets.Checkbox(
                value=False,
                description=f"Select All ({len(fields)})",
                indent=False,
                layout=widgets.Layout(width="300px"),
            )

            # Individual field checkboxes
            field_checkboxes = []
            for field in sorted(fields):
                description = field
                tooltip = self.field_descriptions.get(field, "")

                checkbox = widgets.Checkbox(
                    value=False,
                    description=description,
                    indent=False,
                    layout=widgets.Layout(width="400px"),
                    tooltip=tooltip,
                )
                field_checkboxes.append(checkbox)

            # Store checkboxes for later retrieval
            self.checkboxes[group] = {
                "select_all": select_all,
                "fields": {
                    field: checkbox
                    for field, checkbox in zip(sorted(fields), field_checkboxes)
                },
            }

            # Link "Select All" to toggle all checkboxes in the group
            def make_select_all_handler(group_name):
                def select_all_handler(change):
                    if change["type"] == "change" and change["name"] == "value":
                        for checkbox in self.checkboxes[group_name]["fields"].values():
                            checkbox.value = change["new"]

                return select_all_handler

            select_all.observe(make_select_all_handler(group), names="value")

            # Create the group's widget
            group_widget = widgets.VBox([select_all] + field_checkboxes)
            children.append(group_widget)

        # Set up the accordion
        accordion.children = children
        for i, group in enumerate(self.field_groups.keys()):
            accordion.set_title(i, f"{group} ({len(self.field_groups[group])})")

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

        # Combine all widgets
        ui = widgets.VBox([accordion, get_selected_button, selected_output])

        return ui

    def get_selected_fields(self) -> List[str]:
        """Get the list of selected fields.

        Returns:
            A list of selected field names.
        """
        selected = []
        for group, checkboxes in self.checkboxes.items():
            for field, checkbox in checkboxes["fields"].items():
                if checkbox.value:
                    selected.append(field)
        return selected

    def display(self):
        """Display the field selector UI."""
        self.load_data()
        ui = self.create_ui()
        display(widgets.VBox([self.output, ui]))
