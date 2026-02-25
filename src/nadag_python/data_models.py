import copy
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from os import PathLike
from pathlib import Path
from typing import Any, Literal, Optional, Self, Union

import geopandas as gpd
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator
from shapely.geometry.base import BaseGeometry

from . import get_module_logger
from .config import nadag_config, settings

logger = get_module_logger(__name__)

GeoDataFrameType = Union[gpd.GeoDataFrame, pd.DataFrame]
BoundingBox = (
    list[int | float] | tuple[int | float, int | float, int | float, int | float]
)  # [x_min, y_min, x_max, y_max]


class ModelEnum(StrEnum):
    """
    Mixin to provide 'list_all' and 'dict_all' capabilities to any StrEnum.
    """

    @classmethod
    def list_names(cls) -> list[str]:
        """Returns a list of all enum member names."""
        return [member.name for member in cls]

    @classmethod
    def list_values(cls) -> list[str]:
        """Returns a list of all enum member values."""
        return [member.value for member in cls]

    @classmethod
    def to_dict(cls) -> dict[str, str]:
        """Returns a dictionary mapping name -> value."""
        return {member.name: member.value for member in cls}

    @classmethod
    def print_options(cls) -> None:
        """Prints available options in a readable format."""
        print(f"--- Available {cls.__name__} options ---")
        for name, value in cls.to_dict().items():
            print(f"  {name}: {value}")
        print("-------------------------------------")


@dataclass(frozen=True)
class MethodFields:
    """Definition of the fields related to a geotechnical method, including mapping to API names and references for linking data."""

    name: str  #
    api_name: str  # The name or ID in the API
    metode_key: str  # e.g., metode-KombinasjonSondering
    data_collection: str  # e.g., kombinasjonsonderingdata
    observasjon: str
    id_ref: str
    parent_ref: str


@dataclass(frozen=True)
class SampleFields:
    name: str
    api_name: str
    metode_key: str
    serie_href: str
    data_href: str
    data_id: str
    serie_id: str
    serie_id_ref: str
    href: str


@dataclass(frozen=True)
class ApiSchemaConfig:
    # Definimos los grupos
    geotekniskborehullunders = "geotekniskborehullunders"
    geotekniskborehull = "geotekniskborehull"
    id_field = "identifikasjon.lokalId"
    gbu_ref = "tilhørerGBU.href"
    gbu_id = "tilhørerGBU.title"
    model_gbhu_id = "gbhu_id"
    x = "x"
    y = "y"
    z = "z"
    href = "href"
    feature_id = "lokalid"  # as the API shows, the response comes just as 'id' outside the properties

    tot = MethodFields(
        name="tot",
        api_name="kombinasjonSondering",
        metode_key="metode-KombinasjonSondering",
        data_collection="kombinasjonsonderingdata",
        observasjon="kombinasjonSonderingObservasjon.href",
        id_ref="tilhørerKombinasjonSondering.href",
        parent_ref="tilhørerGBU.href",
    )

    cpt = MethodFields(
        name="cpt",
        api_name="trykkSondering",
        metode_key="metode-Trykksondering",
        data_collection="trykksonderingdata",
        observasjon="trykksonderingObservasjon.href",
        id_ref="tilhørerTrykkSondering.href",
        parent_ref="tilhørerGBU.href",
    )

    rp = MethodFields(
        name="rp",
        api_name="statiskSondering",
        metode_key="metode-StatiskSondering",
        data_collection="statisksonderingdata",
        observasjon="statiskSonderingObservasjon.href",
        id_ref="tilhørerStatiskSondering.href",
        parent_ref="tilhørerGBU.href",
    )

    # Definimos las muestras
    sample = SampleFields(
        name="sa",
        api_name="geotekniskproveseriedel",
        metode_key="metode-GeotekniskPrøveserie",
        serie_href="harPrøveseriedel.href",
        data_href="harData.href",
        serie_id="prøveseriedelId",
        serie_id_ref="tilhørerPrøveseriedel.title",
        href="href",
        data_id=feature_id,  # Assuming the sample data ID is the same as the feature ID, adjust if needed
    )

    # Helper para iterar si es necesario
    @property
    def methods(self):
        return [self.tot, self.cpt, self.rp]

    def get_method_by_type(self, type_key: str) -> MethodFields:
        method = getattr(self, type_key, None)
        if method is None:
            raise ValueError(f"Method type '{type_key}' not found in ApiSchemaConfig.")
        return method

    @property
    def api_url_mapper(self):
        return {item.name: item.api_name.lower() for item in FIELD.methods + [FIELD.sample]}


FIELD = ApiSchemaConfig()


@dataclass(frozen=True)
class NadagData:
    """
    Container for NADAG geospatial and laboratory data.


    """

    bounds: tuple[float, ...]
    fetched_at: datetime = field(default_factory=datetime.now)

    # Location info (Geotekniskborehull/undersPkt, GeotekniskBorehullUnders)
    locations: gpd.GeoDataFrame = field(default_factory=lambda: gpd.GeoDataFrame())
    investigations: gpd.GeoDataFrame = field(default_factory=lambda: gpd.GeoDataFrame())

    # Soundings general info and data (metode-XXXsondering, XXXsonderingObservasjon)
    methods_info: pd.DataFrame = field(default_factory=pd.DataFrame)
    methods_data: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Lab data (Prøveserie, Geotekniskproveseriedel, GeotekniskPrøveseriedelData)
    test_series_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    test_series_aggregated: pd.DataFrame = field(default_factory=pd.DataFrame)

    def is_empty(self) -> bool:
        return self.investigations.empty and self.locations.empty

    def __repr__(self) -> str:
        date_str = self.fetched_at.strftime("%Y-%m-%d %H:%M:%S")
        return (
            f"NadagData(\n"
            f"  bounds = {self.bounds},\n"
            f"  fetched_at = {date_str},\n"
            f"  locations = {len(self.locations)},\n"
            f"  investigations = {len(self.investigations)},\n"
            f"  methods_info = {len(self.methods_info)},\n"
            f"  methods_data = {len(self.methods_data)},\n"
            f"  test_series_data = {len(self.test_series_data)},\n"
            f"  test_series_aggregated = {len(self.test_series_aggregated)}\n"
            f")"
        )

    def print_model(self) -> str:
        """
        Prints the structure of the NadagData model, including the type of each attribute and, for DataFrames, the columns and their data types.
        """
        out_str = ""
        for kk, vv in self.__dict__.items():
            out_str += f"{kk}\n"
            out_str += "-" * 30 + "\n"
            if isinstance(vv, (pd.DataFrame, gpd.GeoDataFrame)):
                out_str += "\n"
                out_str += "\n".join([f" {col}: {dtype}" for col, dtype in zip(vv.columns, vv.dtypes.astype(str))])
            else:
                out_str += " " + str(type(vv)) + "\n"
            out_str += "\n" + "=" * 30 + "\n"
        return out_str

    def export_database(self, path: str | PathLike) -> None:
        """
        Exports the NADAG data to a GeoPackage file. Each DataFrame is saved as a separate layer named after the attribute key.

        Args:
            path (str | PathLike): The file path where the GeoPackage will be saved. The file will be created if it does not exist, or overwritten if it does.

        """
        _path = Path(path)
        _path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the parent directory exists
        for kk, vv in self.__dict__.items():
            if isinstance(vv, gpd.GeoDataFrame):
                vv.to_file(_path, layer=kk)
                logger.debug(f"Exported GeoDataFrame '{kk}' to {_path} with {len(vv)} records.")
            elif isinstance(vv, pd.DataFrame):
                gpd.GeoDataFrame(vv).to_file(_path, layer=kk)
                logger.debug(f"Exported DataFrame '{kk}' to {_path} with {len(vv)} records.")

    # Not sure if this should be here, or in another module, but it is the most convenient place for now since it has access to all the data. It can be refactored later if needed.
    def query_method(self, method_id: str) -> dict[str, Any]:
        """
        Retrieves and combines all relevant information for a specific geotechnical method based on the provided method_id.

            The function performs the following steps:
            1. Filters the `soundings_info` and `soundings_data` DataFrames to find entries matching the given `method_id`.
            2. Extracts the GBHU ID from the filtered `soundings_info` and performs a consistency check with the GBHU ID from the NADAG data.
            3. Retrieves the corresponding investigation and location information based on the GBHU ID.
            4. Renames and filters the sounding data columns according to the `MethodDataDataFrame` mapping.
            5. Combines all the retrieved information into a single dictionary, which includes geometry, method details, location details, investigation details, and the sounding data as a DataFrame.
            6. Returns the combined information as a dictionary. If any of the steps fail (e.g., no matching method_id, GBHU ID mismatch), it logs appropriate warnings and returns an empty dictionary.

        Args:
            method_id (str): The identifier for the geotechnical method to query.
        Returns:
            dict[str, Any]: A dictionary containing the combined information for the specified method, or an
            empty dictionary if the method_id is not found or if there are inconsistencies in the data.
        """
        if self.methods_info.empty:
            logger.warning("Soundings info is empty. Returning empty DataFrame.")
            return {}

        method_id_field = MethodDataFrame.method_id.value
        sounding_info = self.methods_info.query(f"{method_id_field} == @method_id")
        sounding_data = self.methods_data.query(f"{method_id_field} == @method_id").copy()

        if sounding_info.empty:
            logger.warning(f"No sounding info found for method_id {method_id}. Returning empty dict.")
            return {}

        gbhu_id = sounding_info[FIELD.model_gbhu_id].iloc[0]

        # sanity check, wrapped in try/except to not break the function if the field is missing, but log a warning instead
        try:
            gbhu_id_from_nadag = sounding_info["tilhørerGBU.title"].iloc[0]
        except KeyError:
            logger.warning(
                f"'tilhørerGBU.title' not found in soundings_info for method_id {method_id}. Skipping GBHU ID consistency check."
            )
            gbhu_id_from_nadag = None
        finally:
            if gbhu_id != gbhu_id_from_nadag:
                logger.warning(
                    f"GBHU ID mismatch for method_id {method_id}: {gbhu_id} (info) vs {gbhu_id_from_nadag} (data)."
                )
        # --------------

        investigation = self.investigations.loc[self.investigations[FIELD.id_field] == gbhu_id]
        if len(investigation) > 1:
            logger.warning(
                f"Multiple investigations found for GBHU ID {gbhu_id} in method_id {method_id}. Using the first one."
            )
        investigation = investigation.iloc[0]
        location_id = investigation[MethodDataFrame.location_id]
        location = self.locations.loc[self.locations[FIELD.id_field] == location_id].iloc[0]

        sounding_data = sounding_data.rename(columns=MethodDataDataFrame.column_mapper())
        sounding_data = sounding_data.drop(
            columns=[col for col in sounding_data.columns if col not in MethodDataDataFrame.fields()]
        )

        documents = (
            investigation[MethodDataFrame.documents] if MethodDataFrame.documents in investigation.index else None
        )
        interpretations = (
            investigation[MethodDataFrame.interpretations]
            if MethodDataFrame.interpretations in investigation.index
            else None
        )
        # fmt: off
        # this can be a Pydantic Model (Method) with validation and default values, but for simplicity we return a dict for now
        return_dict = {
            MethodDataFrame.geometry.name                : location.geometry, 
            MethodDataFrame.method_id.name               : method_id, 
            MethodDataFrame.gbhu_id.name                 : gbhu_id, 
            MethodDataFrame.location_name.name           : location[MethodDataFrame.location_name], 
            MethodDataFrame.location_id.name             : location_id, 
            MethodDataFrame.depth.name                   : investigation[MethodDataFrame.depth], 
            MethodDataFrame.stop_code.name               : investigation[MethodDataFrame.stop_code], 
            MethodDataFrame.date_investigation_start.name: investigation[MethodDataFrame.date_investigation_start],
            MethodDataFrame.date_adquisition.name        : location[MethodDataFrame.date_adquisition],
            MethodDataFrame.number_of_boreholes.name     : location[MethodDataFrame.number_of_boreholes],
            MethodDataFrame.elevation.name               : location[MethodDataFrame.elevation],
            MethodDataFrame.elevation_reference.name     : location[MethodDataFrame.elevation_reference],
            MethodDataFrame.external_id.name             : location[MethodDataFrame.external_id],
            MethodDataFrame.quick_clay_detection.name    : location[MethodDataFrame.quick_clay_detection],
            MethodDataFrame.description.name             : location[MethodDataFrame.description],
            MethodDataFrame.documents.name               : documents,
            MethodDataFrame.interpretations.name         : interpretations,           
            MethodDataFrame.investigation_area_id.name   : location[MethodDataFrame.investigation_area_id], 
            MethodDataFrame.depth_to_rock_quality.name   : investigation[MethodDataFrame.depth_to_rock_quality], 
            MethodDataFrame.depth_to_rock.name           : investigation[MethodDataFrame.depth_to_rock], 
            MethodDataFrame.method_type_nadag.name       : investigation[MethodDataFrame.method_type_nadag], 
            MethodDataFrame.data.name                    : sounding_data, 
        }
        # fmt: on
        return return_dict

    # samples are aggregated and structured differently than the soundings, so we need a different function to query them.
    # Also, they are not linked to the method_id but to the test_series_id, so we need to query them differently.
    # For simplicity, we will just return a list of dicts with the sample data for a given sample_id (which is the prøveseriedelId in the API).
    # We can later add more functionality if needed, like linking them to the method or the location.
    # Anyway I don't think this will be very useful, We might delete this later.
    def query_sample(self, sample_id: str) -> list[dict[str, Any]]:
        """
        Retrieves sample data for a specific sample_id (prøveseriedelId) from the test_series_data DataFrame.

            The function performs the following steps:
            1. Checks if the `test_series_aggregated` DataFrame is empty. If it is, it logs a warning and returns a list containing an empty dictionary.
            2. If the DataFrame is not empty, it filters the `test_series_data` DataFrame to find entries where the `method_id` matches the provided `sample_id`.
            3. Converts the filtered DataFrame to a list of dictionaries (one for each row) and returns this list.

        Args:
            sample_id (str): The identifier for the sample to query, corresponding to `prøveseriedelId` in the API.
        Returns:
            list[dict[str, Any]]: A list of dictionaries containing the sample data for the specified `sample_id`.
                                  If no data is found or if the aggregated data is empty, it returns a list with a single empty dictionary.

        """

        if self.test_series_aggregated.empty:
            logger.warning("Samples data is empty. Returning empty dict.")
            return [{}]
        # here the samples come set up already so i will be using SampleDataFrame names instead of values for indexing

        method_id_field = SampleDataFrame.method_id

        samples = self.test_series_data.query(f"{method_id_field} == '{sample_id}'")

        return samples.to_dict(orient="records")


class PaginatedResponse(BaseModel):
    """
    Represents a paginated response from the NADAG API.
    This model is designed to handle the structure of the API's JSON responses, including the normalization of nested
    property keys and the grouping of indexed keys into lists. It also provides a method to convert the features into
    a GeoDataFrame for geospatial analysis.

    Attributes:
        type: The type of the response, typically "FeatureCollection".
        numberReturned: The number of features returned in the current page.
        numberMatched: The total number of features that match the query criteria.
        timeStamp: The timestamp when the data was generated or retrieved.
        features: A list of feature dictionaries, each containing geometry and properties.
        links: Optional list of links for pagination (e.g., next page).

    Methods:
        clean_features: A validator that normalizes the properties of each feature upon instantiation.
        _normalize_properties: A static method that handles the normalization of nested and indexed property keys.
        to_gdf: Converts the features into a GeoDataFrame, assuming they are already cleaned
                and normalized. If there are no features, it returns an empty GeoDataFrame.

    """

    type: Literal["FeatureCollection"] = "FeatureCollection"
    numberReturned: Optional[int] = Field(None, ge=0)
    numberMatched: Optional[int] = Field(None, ge=0)
    timeStamp: Optional[str] = None
    features: list[dict[str, Any]] = Field(default_factory=list)
    links: Optional[list[dict[str, Any]]] = None

    @field_validator("features")
    @classmethod
    def clean_features(cls, features: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Clean features upon instantiation.
        """
        cleaned = []
        for feature in features:
            props = feature.get("properties", {})
            feature["properties"] = cls._normalize_properties(props)
            cleaned.append(feature)
        return cleaned

    @staticmethod
    def _normalize_properties(props: dict[str, Any]) -> dict[str, Any]:
        """
        Normalizes nested property keys:
        - Simple nested keys: 'identifikasjon.lokalId' -> Keeps as 'identifikasjon.lokalId'
        - Indexed keys: 'metode.1.title', 'metode.2.title' -> Groups into list under 'metode'
        Runs automatically when PaginatedResponse(**api_data) is called.
        """
        new_props = {}
        lists_buffer = {}
        list_pattern = re.compile(r"^(.+)\.(\d+)\.(.+)$")

        for key, value in props.items():
            match = list_pattern.match(key)
            if match:
                base_name, index, sub_field = match.groups()
                if base_name not in lists_buffer:
                    lists_buffer[base_name] = {}
                if index not in lists_buffer[base_name]:
                    lists_buffer[base_name][index] = {}
                lists_buffer[base_name][index][sub_field] = value
            else:
                new_props[key] = value

        for base_name, indexed_items in lists_buffer.items():
            sorted_indices = sorted(indexed_items.keys(), key=int)
            item_list = [indexed_items[idx] for idx in sorted_indices]
            new_props[base_name] = item_list

        return new_props

    def __len__(self) -> int:
        return len(self.features)

    def to_gdf(self) -> gpd.GeoDataFrame:
        """
        Convert the features to a GeoDataFrame. Assumes that the features are already cleaned and normalized.
        If there are no features, returns an empty GeoDataFrame.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the features with normalized properties.

        """
        if not self.features:
            return gpd.GeoDataFrame()

        crs = settings.DEFAULT_CRS
        return gpd.GeoDataFrame.from_features(self.features, crs=crs)

    @classmethod
    def merge(cls, pages: list[Self]) -> Self:
        if not pages:
            return cls(features=[], links=[], numberMatched=0, numberReturned=0)
        # Combine features and update metadata
        all_features = []
        for page in pages:
            all_features.extend(page.features)

        merged = copy.deepcopy(pages[0])
        merged.features = all_features
        merged.numberMatched = pages[0].numberMatched
        merged.numberReturned = sum(p.numberReturned for p in pages)
        merged.links = []  # Optional: clear links
        return merged


class MethodsConfig:
    CPT_INFO_COLUMNS = nadag_config.methods.cpt_info_columns
    GEOTEKNISKMETODE_TO_METHOD_TYPE_MAPPER = nadag_config.methods.mapper
    SOUNDINGS_FILTER = GEOTEKNISKMETODE_TO_METHOD_TYPE_MAPPER.keys()
    NADAG_FLAG_CODES = nadag_config.methods.flag_codes


class SamplesConfig:
    BRITTLE_KEYWORDS = nadag_config.samples.brittle_keywords
    SAMPLE_FILTER = nadag_config.samples.filter
    brittle_name = nadag_config.samples.classification_names.get("brittle", "quick_clay")
    other_name = nadag_config.samples.classification_names.get("other", "other")
    nothing_name = nadag_config.samples.classification_names.get("nothing", "nothing")


class MethodDataFrame(ModelEnum):
    """
    name (model name)= value (API name / as they come when I use de enum)
    str(MethodDataFrame.location_name) --> 'boreNr' (value)
    """

    geometry = "geometry"
    method_id = "method_id"
    location_id = "underspkt_fk"
    gbhu_id = FIELD.model_gbhu_id
    investigation_area_id = "opprinneligGeotekniskUndersID"
    location_name = "boreNr"
    depth = "boretLengde"
    method_type = "method_type"
    method_type_nadag = "geotekniskMetode"
    stop_code = "stoppKode"
    date_investigation_start = "undersøkelseStart"
    date_adquisition = "datafangstdato"
    number_of_boreholes = "antallBorehullUndersøkelser"
    depth_to_rock = "boretLengdeTilBerg.borlengdeTilBerg"
    depth_to_rock_quality = "boretLengdeTilBerg.borlengdeKvalitet"
    elevation = "høyde"
    elevation_reference = "høydeReferanse"
    external_id = "eksternIdentifikasjon.eksternId"
    quick_clay_detection = "kvikkleirePåvisning"
    documents = "harDokument.href"
    interpretations = "harTolkning.href"
    description = "beskrivelse"
    borehole_azimuth = "boretAzimuth"
    borehole_inclination = "boretHelningsgrad"
    pre_boring_depth = "forboretLengde"
    pre_boring_start_depth = "forboretStartLengde"
    data = "data"
    cpt_info = "cpt_info"

    @classmethod
    def column_mapper(cls):
        return {kk.value: kk.name for kk in cls}

    @classmethod
    def fields(cls):
        return [kk.value for kk in cls]


class Method(BaseModel):
    """
    Detailed model for a specific geotechnical method retrieved via query_method.
    Handles serialization of geopandas/pandas specifics (Shapely geometries, numpy types).
    Not sure if it will be useful to use it, but I let it here just in case.
    The problem is that the fields must be synced with the MethodDataFrame enum, and it can be a bit redundant.
    Maybe we can use the enum to generate the model dynamically in the future.
    """

    geometry: Union[dict, Any] = Field(BaseGeometry, description="GeoJSON compatible geometry")
    method_id: str
    gbhu_id: str
    location_name: Optional[str] = None
    location_id: str
    depth: Optional[float] = None
    stop_code: Optional[int] = None
    date_investigation_start: Optional[datetime] = None
    date_adquisition: Optional[datetime] = None
    number_of_boreholes: Optional[int] = None
    elevation: Optional[float] = None
    elevation_reference: Optional[str] = None
    external_id: Optional[str] = None
    quick_clay_detection: Optional[str] = None
    description: Optional[str] = None
    documents: Optional[str] = None
    interpretations: Optional[str] = None
    investigation_area_id: Optional[str] = None
    depth_to_rock_quality: Optional[float] = None
    depth_to_rock: Optional[float] = None
    method_type_nadag: Optional[str] = None
    data: pd.DataFrame = Field(default_factory=pd.DataFrame, description="Sounding data as DataFrame")

    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)


class MethodDataDataFrame(ModelEnum):
    """
    name (model name)= value (API name)
    """

    penetration_force = "anvendtLast"
    depth = "boretLengde"
    rotation_moment = "dreiemoment"
    penetration_rate = "nedpressinghastighet"
    qc = "nedpressingTrykk"
    fs = "friksjon"
    u2 = "poretrykk"
    penetration_time = "nedpressingTid"
    comment_code = "observasjonKode"
    comment = "observasjonMerknad"
    rotation_rate = "rotasjonHastighet"
    hammering_rate = "slagFrekvens"
    flushing_flow = "spyleMengde"
    flushing_pressure = "spyleTrykk"
    hammering = "hammering"
    increased_rotation_rate = "increased_rotation_rate"
    flushing = "flushing"
    # method_id ... is not unique so it is defined in the column_mapper

    @classmethod
    def column_mapper(cls):
        method_id = ["kombinasjonsondering", "trykksondering", "statisksondering"]
        column_mapper = {kk.value: kk.name for kk in cls}
        for method in method_id:
            column_mapper[method] = "method_id"
        return column_mapper

    @classmethod
    def fields(cls):
        return [kk.name for kk in cls] + ["method_id"]


class SampleDataFrame(ModelEnum):
    location_id = "underspkt_fk"
    sampling_method = "prøveMetode"
    sample_name = "prøveseriedelNavn"
    depth_top = "fraLengde"
    depth_base = "tilLengde"
    depth = "depth"
    method_id = "prøveseriedelId"
    test_series_id = "prøveserieId"
    layer_position = "lagPosisjon"
    lab_analysis = "labAnalyse"
    borehole_length = "boretLengde"
    observation_code = "observasjonKode"
    layer_composition = "layer_composition"
    layer_composition_full = "detaljertLagSammensetning"
    organic_matter = "glødeTap"
    water_content = "vanninnhold"
    axial_deformation = "aksielDeformasjon"
    strength_undrained = "skjærfasthetUdrenert"
    strength_remoulded = "skjærfasthetOmrørt"
    unit_weight = "densitetPrøvetaking"
    liquid_limit = "flyteGrense"
    plastic_limit = "plastitetsGrense"
    strength_undisturbed = "skjærfasthetUforstyrret"
    gbhu_id = "geotekniskborehullunders"  # the value is how it appears in the geotekniskproveserie endpoint, but I get it from the investigations dataframe so it not in use

    geometry = "geometry"
    location_elevation = "høyde"
    location_name = "boreNr"

    @classmethod
    def column_mapper(cls):
        column_mapper = {member.value: member.name for member in cls}
        column_mapper[cls.location_id.name] = cls.location_id.name
        return column_mapper

    @classmethod
    def fields(cls):
        return [kk.name for kk in cls]


class GrundigMethodDataFrame(ModelEnum):
    """
    name (model name)= value (API name)
    """

    method_type = MethodDataFrame.method_type.name
    geometry = MethodDataFrame.geometry.name
    location_name = MethodDataFrame.location_name.name
    data = MethodDataFrame.data.name
    depth = MethodDataFrame.depth.name
    method_id = MethodDataFrame.method_id.name
    location_id = MethodDataFrame.location_id.name
    gbhu_id = MethodDataFrame.gbhu_id.name
    depth_rock = MethodDataFrame.depth_to_rock.name
    depth_rock_quality = MethodDataFrame.depth_to_rock_quality.name
    investigation_area_id = MethodDataFrame.investigation_area_id.name

    @classmethod
    def column_mapper(cls):
        column_mapper = {member.value: member.name for member in cls}
        return column_mapper

    @classmethod
    def fields(cls):
        return [kk.name for kk in cls] + cls.extra_fields()

    @classmethod
    def extra_fields(cls):
        return ["method_status_id", "method_status", "x", "y", "z"]

    @classmethod
    def extra_fields_values(cls):
        # 'x', 'y', 'z' must be computed
        return {
            "method_status_id": 3,
            "method_status": "conducted",
        }


class GrundigSampleDataFrame(ModelEnum):
    strength_remoulded = SampleDataFrame.strength_remoulded.name
    strength_undisturbed = SampleDataFrame.strength_undisturbed.name
    water_content = SampleDataFrame.water_content.name
    strength_undrained = SampleDataFrame.strength_undrained.name
    layer_composition = SampleDataFrame.layer_composition.name
    unit_weight = SampleDataFrame.unit_weight.name
    liquid_limit = SampleDataFrame.liquid_limit.name
    plastic_limit = SampleDataFrame.plastic_limit.name
    name = SampleDataFrame.sample_name.name
    depth_top = SampleDataFrame.depth_top.name
    depth_base = SampleDataFrame.depth_base.name
    method_id = SampleDataFrame.method_id.name
    prøveserieid = SampleDataFrame.test_series_id.name
    geometry = SampleDataFrame.geometry.name
    location_elevation = SampleDataFrame.location_elevation.name
    location_name = SampleDataFrame.location_name.name
    layer_composition_full = SampleDataFrame.layer_composition_full.name
    depth = SampleDataFrame.depth.name
    location_id = SampleDataFrame.location_id.name
    gbhu_id = FIELD.model_gbhu_id

    @classmethod
    def column_mapper(cls):
        column_mapper = {member.value: member.name for member in cls}
        column_mapper[cls.location_id.name] = cls.location_id.name
        return column_mapper

    @classmethod
    def fields(cls):
        return [kk.name for kk in cls] + cls.extra_fields()

    @classmethod
    def extra_fields(cls):
        return [
            FIELD.model_gbhu_id,
            "method_status_id",
            "method_type",
            "method_status",
            FIELD.x,
            FIELD.y,
            FIELD.z,
        ]

    @classmethod
    def extra_fields_values(cls):
        # 'x', 'y', 'z' must be computed
        return {
            "method_status_id": 3,
            "method_type": FIELD.sample.name,
            "method_status": "conducted",
        }
