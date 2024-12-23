import json
from pathlib import Path
from typing import Tuple, Type, Optional, ClassVar, Any

from click import get_app_dir
from pydantic import BaseModel, HttpUrl, FilePath, Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
)

class ClusterConfig(BaseModel):
    """
    Configuration for connecting to a cluster, including endpoint and optional certificate information.
    """

    endpoint: HttpUrl = Field(
        ...,
        title="Cluster Endpoint",
        description="The HTTP URL for the cluster's API endpoint."
    )
    certificate_authority: Optional[FilePath] = Field(
        None,
        title="Certificate Authority File",
        description="Path to the certificate authority file used to verify server certificates."
    )
    client_certificate: Optional[FilePath] = Field(
        None,
        title="Client Certificate File",
        description="Path to the client certificate file used for mutual TLS authentication."
    )
    client_key: Optional[FilePath] = Field(
        None,
        title="Client Key File",
        description="Path to the client key file used for mutual TLS authentication."
    )

class Settings(BaseSettings):
    """
    Main settings model for the application, including cluster configuration and methods
    for loading and saving settings.
    """

    cluster_config: ClusterConfig = Field(
        ..., 
        title="Cluster Configuration",
        description="Configuration object for connecting to the cluster."
    )

    # Default path to the application's configuration file, located in the app directory.
    default_config_file: ClassVar[Path] = Path(get_app_dir(app_name="armonik_cli")) / "config.json"

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        env_prefix="ARMONIK_CLI__",
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
        return (
            init_settings,
            env_settings,
            JsonConfigSettingsSource(
                settings_cls, json_file=cls.default_config_file, json_file_encoding="utf-8"
            ),
        )

    @classmethod
    def load_default_config(cls) -> "Settings":
        """
        Load settings from the default configuration file.

        Returns:
            An instance of the `Settings` class populated with data from the configuration file.
        """
        with cls.default_config_file.open() as file:
            content = file.read()
            return cls(**json.loads(content if content else "{}"))

    def save_default_config(self) -> None:
        """
        Save the current settings to the default configuration file in JSON format.
        """
        with self.default_config_file.open("w") as file:
            file.write(self.model_dump_json(indent=4))

    def get_field(self, path: str) -> Any:
        """
        Retrieve a field from the settings using a dotted string.

        Args:
            path: A dotted string representing the path to the field (e.g., "cluster_config.endpoint").

        Returns:
            The value of the field specified by the path.
        """
        keys = path.split(".")
        model = self
        for key in keys:
            model = getattr(model, key)
        return model

    def set_field(self, path: str, value: Any) -> None:
        """
        Update a field in the settings using a dotted string.

        Args:
            path: A dotted string representing the path to the field (e.g., "cluster_config.endpoint").
            value: The value to set at the specified field.

        Returns:
            None
        """
        keys = path.split(".")
        model = self
        for key in keys[:-1]:
            model = getattr(model, key)
        model = model.model_copy(update={key[-1]: value})
