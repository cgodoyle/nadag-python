import tomllib
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MethodConfigModel(BaseModel):
    """
    Configuration model for methods.

    """

    mapper: dict[str, str] = Field(default_factory=dict)
    flag_codes: dict[str, list[str]] = Field(default_factory=dict)
    cpt_info_columns: list[str] = Field(default_factory=list)
    rock_stop_codes: dict[str, str] = Field(default_factory=dict)


class SampleConfigModel(BaseModel):
    """
    Configuration model for samples.

    """

    brittle_keywords: list[str] = Field(default_factory=list)
    filter: list[str] = Field(default_factory=list)
    classification_names: dict[str, str] = Field(default_factory=dict)


class NadagConfig(BaseModel):
    """
    Configuration model for NADAG API integration, including settings for methods and samples.

    """

    methods: MethodConfigModel
    samples: SampleConfigModel


def load_default_nadag_config() -> NadagConfig:
    """
    Basic NadagConfig
    """
    methods = MethodConfigModel(
        mapper={
            "1": "rp",
            "2": "cpt",
            "15": "tot",
            "16": "tot",
            "17": "rp",
            "18": "rp",
            "36": "tot",
            "37": "tot",
            "41": "rp",
            "46": "rp",
        },
        flag_codes={
            "hammering_starts": ["11", "15", "63"],
            "hammering_ends": ["16", "64"],
            "increased_rotation_rate_starts": ["51"],
            "increased_rotation_rate_ends": ["52"],
            "flushing_starts": ["14", "63"],
            "flushing_ends": ["62", "64"],
        },
        cpt_info_columns=[
            "alpha",
            "rørKappeKorreksjonsFaktor",
            "atmosferiskTrykkKorreksjon",
            "hylseRadieKorreksjon",
            "inSituPoretrykkObservasjon.href",
        ],
    )
    samples = SampleConfigModel(
        brittle_keywords=["quick", "kvikk", "sprøbrudd"],
        filter=[
            "6",
            "47",
            "48",
        ],
        classification_names={
            "brittle": "quick_clay",
            "other": "other",
            "nothing": "nothing",
        },
    )
    return NadagConfig(methods=methods, samples=samples)


def load_nadag_config(filename: str) -> NadagConfig:
    """
    Loads the NADAG configuration from a TOML file. The function searches for the file in multiple locations:
    1. Current working directory or absolute path (if included)
    2. The same directory as this config.py file (package directory)
    3. The project root directory (assuming src/nadag_python/config.py -> go up 2 levels)
    If the file is not found in any of these locations, a FileNotFoundError is raised with details of the search paths.

    """

    # 1. search in current working directory
    cwd_path = Path(filename)
    if cwd_path.exists():
        with open(cwd_path, "rb") as f:
            data = tomllib.load(f)
        return NadagConfig(**data)

    # 2. search in the same directory as this config.py file (package directory)
    package_dir = Path(__file__).parent
    package_path = package_dir / filename
    if package_path.exists():
        with open(package_path, "rb") as f:
            data = tomllib.load(f)
        return NadagConfig(**data)

    # 3. search in the project root (assuming structure src/nadag_python/config.py -> go up 2 levels)
    # package_dir.parent is 'src', package_dir.parent.parent is the project root
    project_root_path = package_dir.parent.parent / filename
    if project_root_path.exists():
        with open(project_root_path, "rb") as f:
            data = tomllib.load(f)
        return NadagConfig(**data)

    # if toml-file is not found, fallback to basic config
    try:
        basic_config = load_default_nadag_config()

        return basic_config
    except Exception as e:
        # If all fails, raise detailed error
        raise FileNotFoundError(
            f"Configuration file '{filename}' not found.\n"
            f"Searched in:\n"
            f"1. {cwd_path.absolute()} (CWD)\n"
            f"2. {package_path.absolute()} (Package Dir)\n"
            f"3. {project_root_path.absolute()} (Project Root)"
            "Basic configuration could not be loaded either due to:"
            f"{e}"
        )


class Settings(BaseSettings):
    """
    Application settings for NADAG API integration, including logging, API endpoints, timeouts, CRS configurations,
    and the path to the NADAG TOML configuration file.

    """

    LOG_LEVEL: str = "ERROR"

    API_BASE_URL: str = "https://geo.ngu.no/api/features/grunnundersokelser_utvidet/collections"
    API_FAKTAARK_URL: str = "https://geo.ngu.no/api/faktaark/nadag/visGeotekniskBorehull.php"

    API_TIMEOUT: int = 500
    API_POOL_TIMEOUT: int = 60
    API_WRITE_TIMEOUT: int = 10
    API_RETRY_ATTEMPTS: int = 3
    API_RETRY_MIN_WAIT: int = 1  # seconds
    API_RETRY_MAX_WAIT: int = 10  # seconds
    API_MAX_CONCURRENCY: int = 50
    API_PAGE_SIZE: int = 100
    API_MAX_DIST_QUERY: int = 2_000  # Default max distance for query in meters

    API_CRS: int = 4326
    DEFAULT_CRS: int = 25833

    NADAG_TOML_PATH: str = "nadag.toml"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="NADAG_PYTHON_",
        env_file_encoding="utf-8",
        extra="allow",
    )


settings = Settings()
nadag_config = load_nadag_config(settings.NADAG_TOML_PATH)


API_VALID_CRS = {
    25833: "http://www.opengis.net/def/crs/EPSG/0/25833",
    25832: "http://www.opengis.net/def/crs/EPSG/0/25832",
    4258: "http://www.opengis.net/def/crs/EPSG/0/4258",
    3857: "http://www.opengis.net/def/crs/EPSG/0/3857",
    4326: "http://www.opengis.net/def/crs/EPSG/0/4326",
}


class CRS(Enum):
    UTM33 = 25833
    UTM32 = 25832
    WGS84 = 4326

    @property
    def url(self):
        return API_VALID_CRS.get(self.value)
