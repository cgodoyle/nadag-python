---
icon: material/database-outline
---

# The `NadagData` dataclass model

## Class Structure
The `NadagData` class is a dataclass that serves as a container for various geotechnical data retrieved from the NADAG API. 

It automatically registers `bounds` delimiting where the data was fetched and `fetched_at` with a timestamp of when it was done. 

The class contains several attributes, each representing a different aspect of the geotechnical data.

- `locations`: A GeoDataFrame containing information about the locations of boreholes, including their geometry and various attributes related to the borehole.
               It maps to the `GeotekniskBorehull` collection of the NADAG API.

- `investigations`: A GeoDataFrame containing information about the geotechnical investigations, including their geometry and various attributes related to the investigation. It maps to the `GeotekniskBorehullUnders` collection of the NADAG API.
  
- `methods_info`: A DataFrame containing metadata about the different geotechnical methods used in the investigations, such as method type and related information.
                  It maps to the `metode-XXX` fields of the `GeotekniskBorehullUnders` collection of the NADAG API, where `XXX` is the type of investigation (e.g. `KombinasjonSondering`, `StatiskSondering`, `TrykkSondering`).

- `methods_data`: A DataFrame containing the actual data from the geotechnical methods, such as measurements and observations.
                  It maps to the `xxxObservasjon` fields of the `GeotekniskBorehullUnders` collection of the NADAG API, where `xxx` is a different spelling of the method (e.g. `kombinasjonSonderingObservasjon`, `statiskSonderingObservasjon`, `trykksonderingObservasjon`).

- `test_series_data`: A DataFrame containing data related to test series, which are specific types of geotechnical investigations that involve multiple samples and tests. It maps to the `metode-GeotekniskPrøveserie` field of the `GeotekniskBorehullUnders` collection of the NADAG API and its related data in `geotekniskproveseriedeldata`.

- `test_series_aggregated`: A GeoDataFrame that aggregates the test series data, providing a spatial representation of the test series along with relevant attributes.
                            This dataframe is already processed and aggregated from the `test_series_data` DataFrame to provide a more standardized (for NVE's purposes) format for analysis and visualization.

The `NadagData` class is designed to provide a structured and organized way to store and access the geotechnical data retrieved from the NADAG API, making it easier for users to work with the data in a consistent manner.


>  The field names of the dataframes are kept 'almost' the same as in the NADAG API to maintain traceability and make it easier to understand where each piece of data comes from. However, some fields have been renamed for consistency (?), e.g. id-fields like `location_id`, `method_id`,  and `cpt_info` which is a container for general cpt information that otherwise would have been missed. I have however struggle with some namings like `gbhu_id` mapping to the `identifikasjon.lokalId` field of the `GeotekniskBorehull` collection. I tried to keep these fields as variables (in the `data_models.py` module) that can be 'easily' changed in the future if needed.


### NadagData dataclass structure
```
bounds
------------------------------
 (tuple)

==============================

fetched_at
------------------------------
  (datetime)

==============================

locations
 (GeoDataFrame)
------------------------------

 geometry: geometry
 datafangstdato: str
 identifikasjon.lokalId: str
 identifikasjon.navnerom: str
 identifikasjon.versjonId: str
 kvalitet.målemetode: object
 oppdateringsdato: str
 antallBorehullUndersøkelser: int64
 beskrivelse: str
 boretLengdeTilBerg.borlengdeKvalitet: str
 boretLengdeTilBerg.borlengdeTilBerg: float64
 boreNr: str
 høyde: float64
 høydeReferanse: str
 opprettetDato: str
 eksternIdentifikasjon.eksternId: str
 eksternIdentifikasjon.eksternNavnerom: str
 eksternIdentifikasjon.eksternVersjonId: str
 eksternIdentifikasjon.eksternLeveringDato: str
 kvikkleirePåvisning: str
 opprinneligGeotekniskUndersID: str
 opphav: str
 maksBoretLengde: float64
 harUndersøkelse.title: str
 harUndersøkelse.href: str
 harDokument.title: str
 harDokument.href: str
 harTolkning.title: str
 harTolkning.href: str

==============================

investigations
  (GeoDataFrame)
------------------------------

 geometry: geometry
 datafangstdato: str
 identifikasjon.lokalId: str
 identifikasjon.navnerom: str
 identifikasjon.versjonId: str
 kvalitet.målemetode: object
 boretAzimuth: float64
 boretHelningsgrad: float64
 boretLengde: float64
 boretLengdeTilBerg.borlengdeKvalitet: str
 boretLengdeTilBerg.borlengdeTilBerg: float64
 opphav: str
 undersøkelseStart: str
 høyde: float64
 høydeReferanse: str
 eksternIdentifikasjon.eksternId: str
 eksternIdentifikasjon.eksternNavnerom: str
 eksternIdentifikasjon.eksternVersjonId: str
 eksternIdentifikasjon.eksternLeveringDato: str
 opprettetDato: str
 geotekniskMetode: str
 forboretLengde: float64
 stoppKode: str
 underspkt_fk: str
 undersPkt.title: str
 undersPkt.href: str
 harDokument.title: str
 harDokument.href: str
 metode-GeotekniskPrøveserie: object
 oppdateringsdato: str
 boreBeskrivelse: str
 forboretStartLengde: float64
 metode-KombinasjonSondering: object
 metode-Trykksondering: object

==============================

methods_info
  (DataFrame)
------------------------------

 identifikasjon.navnerom: str
 identifikasjon.versjonId: str
 boretLengdeTilBerg.borlengdeKvalitet: object
 boretLengdeTilBerg.borlengdeTilBerg: object
 tilhørerGBU.title: str
 kombinasjonSonderingObservasjon.title: str
 method_id: str
 gbhu_id: str
 method_type: str
 inSituPoretrykkObservasjon.title: str
 trykksonderingObservasjon.title: str
 cpt_info: object

==============================

methods_data
  (DataFrame)
------------------------------

 anvendtLast: float64
 boretLengde: float64
 nedpressingTid: float64
 tilhørerKombinasjonSondering.title: str
 observasjonKode: str
 observasjonMerknad: str
 method_type: str
 method_id: str
 friksjon: float64
 poretrykk: float64
 nedpressingTrykk: float64
 tilhørerTrykkSondering.title: str

==============================

test_series_data
  (DataFrame)
------------------------------

 prøveMetode: str
 prøveseriedelNavn: str
 fraLengde: float64
 tilLengde: float64
 prøveseriedelId: str
 prøveserieId: str
 tilhørerPrøveserie.title: str
 tilhørerPrøveserie.href: str
 harData.title: str
 harData.href: str
 location_id: str
 gbhu_id: str
 href: str
 lagPosisjon: str
 labAnalyse: bool
 boretLengde: float64
 observasjonKode: str
 geotekniskproveseriedel: str
 tilhørerPrøveseriedel.title: str
 tilhørerPrøveseriedel.href: str
 lokalid: int64
 detaljertLagSammensetning: str
 glødeTap: float64
 vanninnhold: float64
 aksielDeformasjon: float64
 skjærfasthetUdrenert: float64
 skjærfasthetOmrørt: float64
 densitetPrøvetaking: float64
 flyteGrense: float64
 plastitetsGrense: float64
 skjærfasthetUforstyrret: float64

==============================

test_series_aggregated
  (GeoDataFrame)
------------------------------

 sampling_method: str
 sample_name: str
 depth_top: float64
 depth_base: float64
 method_id: str
 test_series_id: str
 location_id: str
 gbhu_id: str
 layer_position: str
 lab_analysis: bool
 borehole_length: float64
 observation_code: str
 layer_composition_full: str
 organic_matter: float64
 water_content: float64
 axial_deformation: float64
 strength_undrained: float64
 strength_remoulded: float64
 unit_weight: float64
 liquid_limit: float64
 plastic_limit: float64
 strength_undisturbed: float64
 layer_composition: str
 geometry: geometry
 location_name: str
 location_elevation: float64
 depth: float64

==============================
```


### Boreholes and Samples GeoDataFrame structure
The data in `NadagData`can be post-processed to create two GeoDataFrames, one for boreholes and one for samples, with a more standardized format for analysis and visualization, by calling the `get_boreholes_and_samples` method of the `posprocessing` module. 

> The naming convention of the fields in these GeoDataFrames is supposed to be more standardized and consistent, following a more 'Field Manager'-like naming convention, and also more intuitive for users that are not familiar with the NADAG API. However, some fields are still kept with the same name as in the NADAG API just because I lacked creativity or thought it wasn't so important (`prøveserieid`, `gbhu_id`, not sure if there might be more).
> Still, there are field name mappers in the `data_models.py` module that can be easily changed in the future if needed, and also to keep track of where each field is coming from in the NADAG API.

The structure of these GeoDataFrames is as follows:

```
==============================
boreholes
  (GeoDataFrame)
------------------------------
  geometry: geometry
  method_id: str
  gbhu_id: str
  location_name: str
  location_id: str
  depth: float64
  investigation_area_id: str
  depth_rock_quality: str
  depth_rock: float64
  data: object
  method_status_id: int64
  method_status: str
  x: float64
  y: float64
  z: float64
  method_type: str


==============================
samples
  (GeoDataFrame)
------------------------------
  name: str
  depth_top: float64
  depth_base: float64
  method_id: str
  prøveserieid: str
  location_id: str
  gbhu_id: str
  layer_composition_full: str
  unit_weight: float64
  water_content: float64
  strength_undisturbed: float64
  strength_undrained: float64
  liquid_limit: float64
  plastic_limit: float64
  strength_remoulded: float64
  layer_composition: str
  geometry: geometry
  location_name: str
  location_elevation: float64
  depth: float64
  method_status_id: int64
  method_type: str
  method_status: str
  x: float64
  y: float64
  z: float64
```

