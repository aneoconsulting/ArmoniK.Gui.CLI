import json
import rich_click as click

from pathlib import Path
from typing import Union, Dict


class Configuration:
    """
    A class to manage application configuration settings, providing methods
    for loading, creating, updating, and saving the configuration.

    Attributes:
        default_path: The default path to the configuration file.
        _config_keys: The list of valid configuration keys.
        _default_config: The default configuration values.
    """

    default_path = Path(click.get_app_dir("armonik_cli")) / "config"
    _config_keys = ["endpoint"]
    _default_config = {"endpoint": None}

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint

    @classmethod
    def create_default_if_not_exists(cls) -> None:
        """
        Create a default configuration file if it does not already exist.

        This method creates the configuration directory if needed, and writes
        the default configuration to a file if it is not already present.
        """
        cls.default_path.parent.mkdir(exist_ok=True)
        if not (cls.default_path.is_file() and cls.default_path.exists()):
            with cls.default_path.open("w") as config_file:
                config_file.write(json.dumps(cls._default_config, indent=4))

    @classmethod
    def load_default(cls) -> "Configuration":
        """
        Load the configuration from the default configuration file.

        Returns:
            An instance of Configuration populated with values from the default file.
        """
        with cls.default_path.open("r") as config_file:
            return cls(**json.loads(config_file.read()))

    def has(self, key: str) -> bool:
        """
        Check if a specified configuration key is valid.

        Args:
            key: The configuration key to check.

        Returns:
            True if the key is valid, False otherwise.
        """
        return key in self._config_keys

    def get(self, key: str) -> Union[str, None]:
        """
        Retrieve the value of a specified configuration key.

        Args:
            key: The configuration key to retrieve.

        Returns:
            The value of the configuration key, or None if the key is invalid.
        """
        if self.has(key):
            return getattr(self, key)
        return None

    def set(self, key: str, value: str) -> None:
        """
        Set the value of a specified configuration key and save the configuration.

        Args:
            key: The configuration key to set.
            value: The value to assign to the configuration key.
        """
        if self.has(key):
            setattr(self, key, value)
            self._save()

    def to_dict(self) -> Dict[str, str]:
        """
        Convert the configuration to a dictionary format.

        Returns:
            A dictionary representation of the configuration.
        """
        return {key: getattr(self, key) for key in self._config_keys}

    def _save(self):
        """
        Save the current configuration values to the default configuration file.
        """
        with self.default_path.open("w") as config_file:
            config_file.write(json.dumps(self.to_dict(), indent=4))
