#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union

import requests
from airbyte_cdk.models import AirbyteMessage, AirbyteStream, SyncMode
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.source import Source
from airbyte_cdk.sources.utils import casing
from airbyte_cdk.sources.utils.schema_helpers import InternalConfig
from airbyte_cdk.sources.utils.slice_logger import SliceLogger
from airbyte_cdk.sources.utils.transform import TransformConfig, TypeTransformer
from airbyte_cdk.sources.utils.types import JsonType, StreamData


class AbstractStream(ABC):
    @abstractmethod
    def read(
        self,
        cursor_field: Optional[List[str]],
        logger: logging.Logger,
        slice_logger: SliceLogger,
        internal_config: InternalConfig = InternalConfig(),
    ) -> Iterable[StreamData]:
        """
        Read a stream in full refresh mode
        :param stream: The stream to read data from
        :param cursor_field:
        :param logger:
        :param slice_logger:
        :param internal_config:
        :return: The stream's records
        """

    @property
    def logger(self) -> logging.Logger:
        return logging.getLogger(f"airbyte.streams.{self.name}")

    @property
    def name(self) -> str:
        """
        :return: Stream name. By default this is the implementing class name, but it can be overridden as needed.
        """
        return casing.camel_to_snake(self.__class__.__name__)

    def get_error_display_message(self, exception: BaseException) -> Optional[str]:
        """
        Retrieves the user-friendly display message that corresponds to an exception.
        This will be called when encountering an exception while reading records from the stream, and used to build the AirbyteTraceMessage.

        The default implementation of this method does not return user-friendly messages for any exception type, but it should be overriden as needed.

        :param exception: The exception that was raised
        :return: A user-friendly message that indicates the cause of the error
        """
        return None

    @abstractmethod
    def check_availability(self, logger: logging.Logger, source: Optional[Source] = None) -> Tuple[bool, Optional[str]]:
        """
        Checks whether this stream is available.

        :param logger: source logger
        :param source: (optional) source
        :return: A tuple of (boolean, str). If boolean is true, then this stream
          is available, and no str is required. Otherwise, this stream is unavailable
          for some reason and the str should describe what went wrong and how to
          resolve the unavailability, if possible.
        """

    @property
    @abstractmethod
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        """
        :return: string if single primary key, list of strings if composite primary key, list of list of strings if composite primary key consisting of nested fields.
          If the stream has no primary keys, return None.
        """

    @property
    @abstractmethod
    def cursor_field(self) -> Union[str, List[str]]:
        """
        Override to return the default cursor field used by this stream e.g: an API entity might always use created_at as the cursor field.
        :return: The name of the field used as a cursor. If the cursor is nested, return an array consisting of the path to the cursor.
        """

    # TypeTransformer object to perform output data transformation
    transformer: TypeTransformer = TypeTransformer(TransformConfig.NoTransform)

    @lru_cache(maxsize=None)
    @abstractmethod
    def get_json_schema(self) -> Mapping[str, Any]:
        """
        :return: A dict of the JSON schema representing this stream.

        The default implementation of this method looks for a JSONSchema file with the same name as this stream's "name" property.
        Override as needed.
        """

    @property
    def supports_incremental(self) -> bool:
        """
        :return: True if this stream supports incrementally reading data
        """
        return len(self._wrapped_cursor_field()) > 0

    def _wrapped_cursor_field(self) -> List[str]:
        return [self.cursor_field] if isinstance(self.cursor_field, str) else self.cursor_field

    @classmethod
    def parse_response_error_message(cls, response: requests.Response) -> Optional[str]:
        """
        Parses the raw response object from a failed request into a user-friendly error message.
        By default, this method tries to grab the error message from JSON responses by following common API patterns. Override to parse differently.

        :param response:
        :return: A user-friendly message that indicates the cause of the error
        """

        # default logic to grab error from common fields
        def _try_get_error(value: Optional[JsonType]) -> Optional[str]:
            if isinstance(value, str):
                return value
            elif isinstance(value, list):
                errors_in_value = [_try_get_error(v) for v in value]
                return ", ".join(v for v in errors_in_value if v is not None)
            elif isinstance(value, dict):
                new_value = (
                    value.get("message")
                    or value.get("messages")
                    or value.get("error")
                    or value.get("errors")
                    or value.get("failures")
                    or value.get("failure")
                    or value.get("detail")
                )
                return _try_get_error(new_value)
            return None

        try:
            body = response.json()
            return _try_get_error(body)
        except requests.exceptions.JSONDecodeError:
            return None

    @property
    def namespace(self) -> Optional[str]:
        """
        Override to return the namespace of this stream, e.g. the Postgres schema which this stream will emit records for.
        :return: A string containing the name of the namespace.
        """
        return None

    def as_airbyte_stream(self) -> AirbyteStream:
        stream = AirbyteStream(name=self.name, json_schema=dict(self.get_json_schema()), supported_sync_modes=[SyncMode.full_refresh])

        if self.namespace:
            stream.namespace = self.namespace

        if self.supports_incremental:
            stream.source_defined_cursor = self.source_defined_cursor
            stream.supported_sync_modes.append(SyncMode.incremental)  # type: ignore
            stream.default_cursor_field = self._wrapped_cursor_field()

        keys = AbstractStream._wrapped_primary_key(self.primary_key)
        if keys and len(keys) > 0:
            stream.source_defined_primary_key = keys

        return stream

    @staticmethod
    def is_record(record_data_or_message: StreamData) -> bool:
        if isinstance(record_data_or_message, dict):
            return True
        elif isinstance(record_data_or_message, AirbyteMessage):
            return bool(record_data_or_message.type == MessageType.RECORD)
        else:
            return False

    @property
    @abstractmethod
    def source_defined_cursor(self) -> bool:
        """
        Return False if the cursor can be configured by the user.
        """

    @staticmethod
    def _wrapped_primary_key(keys: Optional[Union[str, List[str], List[List[str]]]]) -> Optional[List[List[str]]]:
        """
        :return: wrap the primary_key property in a list of list of strings required by the Airbyte Stream object.
        """
        if not keys:
            return None

        if isinstance(keys, str):
            return [[keys]]
        elif isinstance(keys, list):
            wrapped_keys = []
            for component in keys:
                if isinstance(component, str):
                    wrapped_keys.append([component])
                elif isinstance(component, list):
                    wrapped_keys.append(component)
                else:
                    raise ValueError(f"Element must be either list or str. Got: {type(component)}")
            return wrapped_keys
        else:
            raise ValueError(f"Element must be either list or str. Got: {type(keys)}")
