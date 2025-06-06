---
title: "Space2Stats API Demo in R"
output:
  html_document:
    df_print: paged
---
# R Example

```R name="setup" tags=["remove_cell"]
knitr::opts_chunk$set(echo = TRUE)
library(httr2)
library(jsonlite)
library(sf)
library(dplyr)
library(leaflet)
library(viridis)
```

## Set Up API Endpoints

```R
base_url <- "https://space2stats.ds.io"
fields_endpoint <- paste0(base_url, "/fields")
summary_endpoint <- paste0(base_url, "/summary")
```

## Fetch Available Fields

```R
# Set up the request to fetch available fields
req <- request(base_url) |>
  req_url_path_append("fields")  # Append the correct endpoint

# Perform the request and get the response
resp <- req |> req_perform()

# Check the status code
if (resp_status(resp) != 200) {
    stop("Failed to get fields: ", resp_body_string(resp))
}

# Parse the response body as JSON
available_fields <- resp |> resp_body_json()

# Print the available fields in a simplified format
print("Available Fields:")
print(unlist(available_fields))
```

## Define Area of Interest (AOI)

```R
minx <- 29.038924
miny <- -4.468958
maxx <- 30.850461
maxy <- -2.310523

# Define Area of Interest (AOI) with NULL for properties to ensure it's treated as a valid dictionary
aoi <- list(
  type = "Feature",
  properties = NULL,  # Empty properties
  geometry = list(
    type = "Polygon",
    coordinates = list(
      list(
        c(minx, maxy),
        c(minx, miny),
        c(maxx, miny),
        c(maxx, maxy),
        c(minx, maxy)
      )
    )
  )
)
```

## Request Summary Data

```R
request_payload <- list(
    aoi = aoi,
    spatial_join_method = "centroid",
    fields = list("sum_pop_2020"),
    geometry = "point"
)

# Set up the base URL and create the request
req <- request(base_url) |>
  req_url_path_append("summary") |>
  req_body_json(request_payload)

# Perform the request and get the response
resp <- req |> req_perform()

# Turn response into a data frame
summary_data <- resp |> resp_body_string() |> fromJSON(flatten = TRUE)

# Extract coordinates and convert to a spatial data frame (sf object)
summary_data <- summary_data %>%
  mutate(
    x = sapply(geometry, function(g) fromJSON(g)$coordinates[1]),
    y = sapply(geometry, function(g) fromJSON(g)$coordinates[2])
  )

# Convert to sf, drop extra geometry fields
gdf <- st_as_sf(summary_data, coords = c("x", "y"), crs = 4326)
```

## Visualization

```R

# Replace NA values in sum_pop_2020 with 0
gdf$sum_pop_2020[is.na(gdf$sum_pop_2020)] <- 0

# Create a custom binned color palette with non-uniform breaks
# For example: 0 (distinct color), 1-200000 (gradient), 200001+ (another color)
breaks <- c(0, 1, 1000, 10000, 50000, 100000, 200000, max(gdf$sum_pop_2020))

custom_pal <- colorBin(palette = c("lightgray", "yellow", "orange", "red", "purple", "blue"),
                       domain = gdf$sum_pop_2020, bins = breaks)

# Create the leaflet map with custom binned coloring
leaflet(gdf) %>%
  addTiles() %>%  # Add default OpenStreetMap map tiles
  addCircleMarkers(
    radius = 3,  # Adjust size as needed
    color = ~custom_pal(sum_pop_2020),
    stroke = FALSE, fillOpacity = 0.7,
    popup = ~paste("Hex ID:", hex_id, "<br>", "Population 2020:", sum_pop_2020)  # Add a popup with details
  ) %>%
  addLegend(
    pal = custom_pal, values = gdf$sum_pop_2020, title = "Population 2020 (Custom Binned Scale)",
    opacity = 1
  )
```
