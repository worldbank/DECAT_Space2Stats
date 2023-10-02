# Poverty and climate


## Data availability

### Flood

The data came from FATHOM [Global Flood Map](https://www.fathom.global/product/global-flood-map/), it provides water height in meters, by pixel with 30m spatial resolution. FATHOM provides 3 type different scenario: (i) Undefended, (ii) Defended and (iii) Pluvial with various return periods.

Pluvial Flood with 100-year return period are commonly used by various acitivities within the Bank as proxy for Flood Risk.

### Drought

GOST has various index to measure a drought:

**Meteorological Drought**

* Standardize Precipitation Index (SPI), 1958-2022, monthly, timescale: 1-72months, 4km spatial resolution, Source: TerraClimate
* Standardize Precipitation-Evapotranspiration Index (SPEI), 1958-2022, monthly, timescale: 1-72months, 4km spatial resolution, Source: TerraClimate
* Standardize Precipitation Index (SPI), 1981-2023, monthly, timescale: 1-72months, 5.6km spatial resolution, Source: CHIRPS
* Number of Consecutive Dry Days (CDD), 2000-2023, daily, 11km spatial resolution, Source: IMERG

As example, 40-years drought analysis for Morocco based on CHIRPS's SPI. From this analysis, we can derive the number of drought event, magnitude, duration and severity. https://datapartnership.org/morocco-economic-monitor/docs/drought-eo.html

Proposed variable: **Hydrological Drought**

* Standardize Runoff Index (SRI), 1958-2022, monthly, timescale: 1-72months, 4km spatial resolution, Source: TerraClimate. 

As the data is not available yet, if required GOST need to compute this variable.

### Heatwaves

Based on ERA5-Land daily temperature data (1981-2022), with 11km spatial resolution, we have:  

* Number of hot days (daily max temperature exceeding 35degC), available monthly and annual.
* Number of heatwaves per time period, available monthly and annual. It calculated following the World Meteorological Organization (WMO), defines a heat wave as five or more consecutive days of prolonged heat in which the daily maximum temperature is higher than the average maximum temperature by 5°C (9°F) or more.

### Cyclone

NOAA IBTrACS cyclone track from 1901-2022 are available in point and line shapefiles. CSV and NetCDF also available at S3. 

### Wildfire 

Global Fire Emissions Database ([GFED](https://www.globalfiredata.org/)) listed as one of potential data will be use.

### Pollution

Modeled PM2.5 data from the University of Washington are propose for this activities (Confirmation and follow up are needed).

### Weather variables.

Monthly rainfall, (min, mean, max) temperature, relative humidity, wind speed, evaporation, potential evaporation, surface pressure, runoff, soil water and solar radiation from ERA5-Land with period 1950-2022, available in NetCDF format. S3 [link](s3://wbgdecinternal-ntl/climate/data/era5land)

### Sea Level Rise

Sea Level Rise (SLR) historical data is not available, but CCKP has the projection data for all SSP's scenario.

### Climate Projection

[CCKP](https://climateknowledgeportal.worldbank.org/) host climate projection data on their website. To understand what available, check this [summary](https://github.com/bennyistanto/awesome-climate-data/blob/main/cckp/README.md)

Proposed variables: projections for extreme events, e.g. probability of heatwaves, droughts, etc.
