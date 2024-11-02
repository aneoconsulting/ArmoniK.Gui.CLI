import json
import logging

import click
import jsonschema

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Union, Any, Optional

from armonik.common import Session, TaskOptions
from google._upb._message import ScalarMapContainer
from rich.logging import RichHandler


APP_NAME = "armonik"
MSG_FORMAT = "%(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class ConfigFile:
    _schema = schema = {
        "type": "object",
        "properties": {
            "endpoint": {"anyOf": [{"type": "string", "format": "hostname"},{"type": "null"}]},
            "certificate_authority": {"anyOf": [{"type": "string", "format": "uri-reference"},{"type": "null"}]},
            "client_certificate": {"anyOf": [{"type": "string", "format": "uri-reference"},{"type": "null"}]},
            "client_key": {"anyOf": [{"type": "string", "format": "uri-reference"},{"type": "null"}]},
        },
        "required": [],
    }
    default_path = Path(click.get_app_dir(app_name=APP_NAME)) / "config"

    def __init__(self, loc: Optional[str] = None) -> None:
        self.loc = Path(loc) if loc is not None else self.default_path

    def create_if_not_exists(self):
        self.loc.parent.mkdir(exist_ok=True)
        self.loc.touch()
        with self.loc.open("w") as config_file:
            config_file.write(json.dumps({}))

    def load(self) -> dict[str, str]:
        if not self.loc.exists() and self.loc == self.default_path:
            self.create_if_not_exists()
        with self.loc.open("r") as config_file:
            data = json.loads(config_file.read())
            jsonschema.validate(instance=data, schema=self._schema)
            return data


def reconcile_connection_details(
    cli_params: Dict[str, Optional[Union[str, Path]]],
    optional_config_file: Optional[Path],
    logger: logging.Logger,
) -> Dict[str, Union[str, Path]]:
    """
    Reconciles parameters from command-line, optional config file, and default config file.
    Command-line params have highest priority, then optional config, then default config.

    Args:
        cli_params (Dict[str, Optional[str]]): Parameters provided via command-line.
        optional_config_file (str): Path to the optional config file.
        default_config_file (str): Path to the default config file.

    Returns:
        Dict[str, str]: Final parameters with reconciled values.
    """

    # Read config files in priority order: optional config file, then default config
    if optional_config_file:
        optional_config = ConfigFile(optional_config_file).load()
        logger.debug(f"Loaded optional config file: {optional_config_file}")
    else:
        optional_config = None

    default_config = ConfigFile().load()
    logger.debug(f"Loaded default config file: {ConfigFile.default_path}")

    final_params = {}

    # Resolve each parameter in priority order
    for key in cli_params.keys():
        # Priority 1: command-line parameters
        if cli_params.get(key) is not None:
            ctx = click.get_current_context()
            param_source = ctx.get_parameter_source(param_name)
            if param_source == click.core.ParameterSource.COMMANDLINE:
                source = "command-line"
            elif param_source == click.core.ParameterSource.ENVIRONMENT:
                source = "environment variable"
            final_params[key] = cli_params[key]
            logger.info(f"Parameter '{key}' retrieved from {source}.")
        # Priority 2: optional config file, if exists
        elif optional_config and optional_config.get(key) is not None:
            final_params[key] = optional_config[key]
            logger.info(f"Parameter '{key}' retrieved from config file {optional_config.loc}.")
        # Priority 3: default config file
        elif default_config.get(key) is not None:
            final_params[key] = default_config[key]
            logger.info(f"Parameter '{key}' taken from default config file.")
        else:
            if key == "endpoint":
                raise click.exceptions.UsageError("No endpoint provided.")
            final_params[key] = None
            logger.info(f"Parameter '{key}' is missing.")

    return final_params


def get_logger(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level, format=MSG_FORMAT, datefmt=DATE_FORMAT, handlers=[RichHandler()]
    )
    logging.captureWarnings(True)
    return logging.getLogger("armonik_cli")


class CLIJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder to handle the display of data returned by ArmoniK's Python API as pretty
    JSONs.

    Attributes:
        __api_types (list): The list of ArmoniK API Python objects managed by this encoder.
    """

    __api_types = [Session, TaskOptions]

    def default(self, obj: object) -> Union[str, Dict[str, Any]]:
        """
        Override the `default` method to serialize non-serializable objects to JSON.

        Args:
            obj: The object to be serialized.

        Returns:
            str or dict: The object serialized.
        """
        if isinstance(obj, timedelta):
            return str(obj)
        elif isinstance(obj, datetime):
            return str(obj)
        # The following case should disappear once the Python API has been corrected by correctly
        # serializing the associated gRPC object.
        elif isinstance(obj, ScalarMapContainer):
            return json.loads(str(obj).replace("'", '"'))
        elif any([isinstance(obj, api_type) for api_type in self.__api_types]):
            return {self.camel_case(k): v for k, v in obj.__dict__.items()}
        else:
            return super().default(obj)

    @staticmethod
    def camel_case(value: str) -> str:
        """
        Convert snake_case strings to CamelCase.

        Args:
            value (str): The snake_case string to be converted.

        Returns:
            str: The CamelCase equivalent of the input string.
        """
        return "".join(word.capitalize() for word in value.split("_"))
