# Geospatial Enhancement of Surveys
[For more project information, please visit project website](https://worldbank.github.io/DECAT_HH_Geovariables/README.html)

This repository contain code and documentation about a collection of activities whose overarching goal is to add geospatial variables to locations from household surveys. For example, given a completed household survey in a country, we can generate anonymized household level coordinates (or enumeration area level coordinates which will be centroids) and link them with variables coming from geospatial data such as precipitation, vegetation indices and more which are otherwise not avaibale in the survey itself. Thus, geoenhancement is a way to enrich survey data with geospatial variables so that analysts can conduct more extended analysis. The repository provides the following:

Survey geo-enhacment process. In-depth information about how the geovariables are generated, rationale for selection of data sources and other design decisions. In addition, we also document best practices for this type of data processing.

Data generation for specific surveys. All the required documentation about each survey which has gone through this geo-enhancement is fully covered in this repo. This includes what geovariables were generated, where to find the output geovariables and more.

Spatial anonymization. As you will note from the survey geo-enhancement process, the survey coordinates need to be anonymized first before they are used in the ge-enhancement process and the associated geovariables publicly disseminated. As such, the work covered in this repository included development of tools for robust spatial anonymization. A Python package: [spatial-anoanonymization] (worldbank/Spatial-Anonymization) for this prupose is being developed. In this regard, information about this package and other tools for spatial anonymization and bets practices will also be provided.

```{figure} ../images/geo-enhancement-pipeline.png
---
alt: Geo-enhancement process
---
Geo-enhancement workflow
```
