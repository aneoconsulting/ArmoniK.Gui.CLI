import json

from datetime import datetime, timedelta
from typing import Dict, Union, Any

from armonik.common import Session, TaskOptions
from google._upb._message import ScalarMapContainer
from pydantic import BaseModel, HttpUrl


class CLIJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder to handle the display of data returned by ArmoniK's Python API as pretty
    JSONs.

    Attributes:
        __api_types: The list of ArmoniK API Python objects managed by this encoder.
    """

    __api_types = [Session, TaskOptions]

    def default(self, obj: object) -> Union[str, Dict[str, Any]]:
        """
        Override the `default` method to serialize non-serializable objects to JSON.

        Args:
            The object to be serialized.

        Returns:
            The object serialized.
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
        elif isinstance(obj, BaseModel):
            return json.loads(obj.model_dump_json())
        elif isinstance(obj, HttpUrl):
            return str(obj)
        else:
            return super().default(obj)

    @staticmethod
    def camel_case(value: str) -> str:
        """
        Convert snake_case strings to CamelCase.

        Args:
            value: The snake_case string to be converted.

        Returns:
            The CamelCase equivalent of the input string.
        """
        return "".join(word.capitalize() for word in value.split("_"))
