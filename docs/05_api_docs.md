---
icon: material/api
---

# Library documentation

## HTTP client (nadag_python.http_client)
Client for making HTTP requests to the NADAG API, handling pagination, rate limiting, and retries.

::: nadag_python.http_client

## NADAG functions (nadag_python.nadag_functions)
Functions for fetching and processing data from the NADAG API, including functions for retrieving investigations, locations, methods, and test series data, as well as functions for post-processing and structuring the data into a more usable format. (:thinking:)

::: nadag_python.nadag_functions

## Postprocessing functions (nadag_python.postprocessing)
Functions for post-processing the raw data retrieved from the NADAG API, including functions for cleaning, structuring, and simplifying the data, as well as functions for exporting the data to a structured GeoDataFrame format similar to NVE's Grundig / Field Manager structure. (:thinking:)

::: nadag_python.postprocessing


## Utility functions (nadag_python.utils)
Utility functions for various tasks such as logging, configuration management, and other helper functions used throughout the library.

::: nadag_python.utils

<!-- Not sure if this should be included, we must anyways make a page explaining the data model. -->
## Data models (nadag_python.data_models)
Data models for representing the geotechnical data retrieved from the NADAG API, including dataclasses for investigations, locations, methods, test series, and aggregated test series data. 
It also includes field definitions and type annotations for the data, as well as functions for converting raw API responses into structured data models (:thinking:).

::: nadag_python.data_models


## Configuration (nadag_python.config)
Configuration management for the library, including functions for loading and managing configuration settings such as API endpoints and parameters, 
data structuring options, and other settings that can be customized by the user.
The configuration are based in a `pydantic-settings` model, which allows overriding settings via environment variables, which is useful for deployment and production use cases.
Configuration associated with functional aspects of the library as mappers, filters, etc are defined in a toml file at project root (`nadag.toml`), which is loaded and managed by the configuration module.

### Environment variables
```bash
LOG_LEVEL = "DEBUG"
API_BASE_URL = "https://geo.ngu.no/api/features/grunnundersokelser_utvidet/collections"
API_FAKTAARK_URL = "https://geo.ngu.no/api/faktaark/nadag/visGeotekniskBorehull.php"
API_TIMEOUT = 300
API_DATA_LIMIT = 10000  # Default limit for soundings data fetching
API_RETRY_ATTEMPTS = 3
API_RETRY_MIN_WAIT = 1  # seconds
API_RETRY_MAX_WAIT = 10  # seconds
API_MAX_CONCURRENCY = 50
API_MAX_DIST_QUERY = 500  # Default max distance for query in meters
API_CRS = 4326
DEFAULT_CRS = 25833
NADAG_TOML_PATH = "nadag.toml"

```
### Nadag TOML configuration file
```toml
[methods.mapper] 
    # Mapping of method type codes (from NADAG API) to method names (of our model)
    "1"     = "rp"  #  Dreietrykksondering
    "2"     = "cpt"  #  Trykksondering (CPT, CPTU)
    "15"    = "tot"  #  Totalsondering Norge

    # more

[methods.flag_codes] 
    # Codes for different flags in the methods data, such as hammering, increased rotation rate, 
    # and flushing, which can be used to identify specific conditions during the sounding process.

    "hammering_starts"                  = ["11", "15", "63"]
    "hammering_ends"                    = ["16", "64"]
    "increased_rotation_rate_starts"    = ["51"]
    "increased_rotation_rate_ends"      = ["52"]
    "flushing_starts"                   = ["14", "63"]
    "flushing_ends"                     = ["62", "64"]

[methods]
    # Columns from the methods_info dataframe to be included in the final methods GeoDataFrame, 
    # which can provide additional context and information about the soundings.
    cpt_info_columns = [
        "alpha", 
        "rørKappeKorreksjonsFaktor", 
        "atmosferiskTrykkKorreksjon", 
        "hylseRadieKorreksjon", 
        "inSituPoretrykkObservasjon.href"
        ]  

[samples]

    # Keywords to identify brittle samples (quick clay) based on the description or other text 
    # fields in the samples data.
    brittle_keywords    = ["quick", "kvikk", "sprøbrudd"] 
    
    # Codes for filtering the samples data, which can be used to include or exclude specific types of 
    # samples based on their classification or other criteria.
    filter              = [
                            "6",  # Kjerneprøve
                            "47",  # Prøve uspesifisert
                            "48",  # Prøveserie uspesifisert
                        ] 

[samples.classification_names] 
    # Mapping of sample classification codes to human-readable names, which can be used to 
    # interpret the classification # of samples in the data and provide more meaningful information 
    # about the types of samples retrieved from the NADAG API.
    brittle        = "quick_clay"
    other          = "other"
    nothing        = "nothing"
```
