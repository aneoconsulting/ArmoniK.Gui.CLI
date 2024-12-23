import json
import logging

from enum import Enum
from functools import partial
import logging.config
from pathlib import Path
from typing import Annotated, Dict, Optional, Tuple, Union, ClassVar, List, Type
from typing_extensions import Self

from armonik.common import Direction, Session
from armonik.common.channel import create_channel
from pydantic import BaseModel, Field, model_validator, HttpUrl, FilePath, AfterValidator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
)
from rich_click import get_app_dir, RichContext


from armonik.common.filter import SessionFilter, Filter
from armonik.common.filter.filter import FType

from armonik_cli.exceptions import ConfigFileFormatError
from armonik_cli.utils import ContextFilter, LogMsgStripFilter


def _remove_suffix(s: str, suffix: str) -> str:
    if s.endswith(suffix):
        return s[:-len(suffix)]
    return s


def _is_valid_field(value: str, filter: Filter) -> str:
    if value in filter._fields and not (filter._fields[value][0] == FType.NA or filter._fields[value][0] == FType.UNKNOWN):
        return value
    raise ValueError(f"{value} is not a valid field for {_remove_suffix(filter.__name__, 'Filter')}.")


def _are_valid_cols_is_valid_field(value: str, filter: Filter) -> List[Tuple[str, str]]:
    for field in value:
        _is_valid_field(field, filter)
    return value


class OutputFormat(str, Enum):
    AUTO = "auto"
    JSON = "json"
    YAML = "yaml"
    TABLE = "table"


class ClusterConfig(BaseModel):
    endpoint: HttpUrl = Field("http://10.43.107.198:5001", description="The endpoint URL for the cluster. Must be a valid HTTP or HTTPS URL.")
    certificate_authority: Optional[Union[str, bytes, FilePath]] = Field(
        None, description="The certificate authority, provided as a string, bytes, or a file path.")
    client_certificate: Optional[Union[str, bytes, FilePath]] = Field(
        None, description="The client certificate, provided as a string, bytes, or a file path.")
    client_key: Optional[Union[str, bytes, FilePath]] = Field(
        None, description="The client key, provided as a string, bytes, or a file path.")


class Settings(BaseSettings):
    cluster_config: ClusterConfig = Field(ClusterConfig(), description="Cluster configuration.")
    debug: bool = Field(False, description="Enable debug mode")
    verbose: bool = Field(False, description="Enable verbose mode")
    output_format: OutputFormat = Field(OutputFormat.AUTO, description="CLI output format")
    session_table_cols: Annotated[Dict[str, str], AfterValidator(partial(_are_valid_cols_is_valid_field, filter=SessionFilter))] = Field(
        default={"session_id": "ID", "status": "Status", "created_at": "Created At"},
        description="Columns to display for sessions.",
    )
    session_sort_field: Annotated[str, AfterValidator(partial(_is_valid_field, filter=SessionFilter))] = Field("created_at", description="Default session sort field")
    session_sort_direction: Direction = Field(Direction.DESC, description="Default session sort direction")

    # Default path to the application's configuration file, located in the app directory.
    user_config_file: ClassVar[Path] = Path(get_app_dir(app_name="armonik_cli")) / "config.json"
    additional_config_file: ClassVar[Optional[Path]] = None

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        env_prefix="ARMONIK__",
        nested_model_default_partial_update=True,
        validate_assignment=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        """
        Customize the order of settings sources for the application. JSON configuration files
        are added as a source after environment variables.
        """
        try:
            user_config_settings = JsonConfigSettingsSource(
                settings_cls, json_file=cls.user_config_file, json_file_encoding="utf-8"
            )
        except json.decoder.JSONDecodeError as error:
            raise ConfigFileFormatError(f"{cls.user_config_file} invalid format: {str(error)}.")
        if cls.additional_config_file is None:
            return (
                init_settings,
                env_settings,
                user_config_settings,
            )
        try:
            additional_config_settings = JsonConfigSettingsSource(
                settings_cls, json_file=cls.additional_config_file, json_file_encoding="utf-8"
            )
        except json.decoder.JSONDecodeError as error:
            raise ConfigFileFormatError(f"{cls.additional_config_file} invalid format: {str(error)}.")
        return (
            init_settings,
            env_settings,
            additional_config_settings,
            user_config_settings,
        )

    @classmethod
    def from_click_inputs(cls, ctx: RichContext) -> "Settings":
        commands = ctx.command_path
        params = ctx.params
        import pdb; pdb.set_trace()
        cls.additional_config_file = params.get("additional_config", None)
        return cls()

    def get_log_level(self):
        if self.debug:
            return logging.DEBUG
        elif self.verbose:
            return logging.INFO
        else:
            return logging.WARNING

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            logging.captureWarnings(True)

            # Define handlers
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.get_log_level())
            console_handler.setFormatter(logging.Formatter(
                fmt="%(asctime)s.%(msecs)03dZ%(levelname)s [%(funcName)s] | "
                    '{"message": "%(message)s", "filename": "%(filename)s", "line": %(lineno)d, '
                    '"context": %(context)s}',
                datefmt="%Y-%m-%dT%H:%M:%S",
            ))
            console_handler.addFilter(LogMsgStripFilter())
            console_handler.addFilter(ContextFilter())

            # Create logger
            self._logger = logging.getLogger("armonik_cli")
            self._logger.setLevel(self.get_log_level())
            self._logger.addHandler(console_handler)
            self._logger.propagate = False

        return self._logger

    @property
    def channel_ctx(self):
        return create_channel(
            self.cluster_config.endpoint,
            certificate_authority=self.cluster_config.certificate_authority,
            client_certificate=self.cluster_config.client_certificate,
            client_key=self.cluster_config.client_key
        )


class UserConfigFile:
    path: Path

    def create_if_not_exists():
        pass

    def set():
        pass

    def unset():
        pass

    def get():
        pass

    def get_cluster():
        pass

    def set_cluster():
        pass

    def unset_cluster():
        pass


Settings.additional_config_file = Path.home() / ".config/armonik_cli/add.json"
settings = Settings(session_sort_field="session_id")
print(settings.session_sort_field)
