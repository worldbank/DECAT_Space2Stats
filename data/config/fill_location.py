import json

# Load data from iso3.json
with open("iso3.json", "r") as f:
    iso3_data = json.load(f)

# Load the template
with open("location_template.json", "r") as f:
    location_template = json.load(f)

# Create a filled configuration
location_filled = {"locations": []}
for iso3, country_name in iso3_data.items():
    location = location_template["locations"][0].copy()
    location["ISO3"] = iso3
    location["country_name"] = country_name
    location_filled["locations"].append(location)

# Save the filled configuration
with open("location_filled.json", "w") as f:
    json.dump(location_filled, f, indent=2)
