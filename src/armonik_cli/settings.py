import json

from pathlib import Path
from typing import Tuple, Type, Optional, ClassVar, List, Any

from click import get_app_dir
from pydantic import BaseModel, HttpUrl, Field, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict, JsonConfigSettingsSource, PydanticBaseSettingsSource


class ClusterConfig(BaseModel):
    endpoint: HttpUrl
    certificate_authority: Optional[FilePath] = None
    client_certificate: Optional[FilePath] = None
    client_key: Optional[FilePath] = None


class Settings(BaseSettings):
    cluster_config: ClusterConfig

    default_config_file: ClassVar = Path(get_app_dir(app_name="armonik_cli")) / "config.json"
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
        return (
            init_settings,
            env_settings,
            JsonConfigSettingsSource(
                settings_cls,
                json_file=cls.default_config_file,
                json_file_encoding="utf-8"
            ),
        )

    @classmethod
    def load_default_config(cls) -> "Settings":
        with cls.default_config_file.open() as file:
            content = file.read()
            return cls(**json.loads(content if content else "{}"))

    def save_default_config(self) -> None:
        with self.default_config_file.open("w") as file:
            file.write(self.model_dump_json())

    def get_field(self, path: str) -> Any:
        """Retrieve a field from the settings using a dotted string."""
        keys = path.split(".")
        model = self
        for key in keys:
            model = getattr(model, key)

        return model

    def set_field(self, path: str, value: Any) -> Any:
        """Retrieve a field from the settings using a dotted string."""
        keys = path.split(".")
        model = self
        for key in keys[:-1]:
            model = getattr(model, key)
        model = model.model_copy(update={key[-1]: value})
