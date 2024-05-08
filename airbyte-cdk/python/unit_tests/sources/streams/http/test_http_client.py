# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from logging import Logger
from unittest.mock import MagicMock, patch

import pytest
import requests
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.call_rate import APIBudget, CachedLimiterSession, LimiterSession
from airbyte_cdk.sources.streams.http import HttpClient
from airbyte_cdk.sources.streams.http.error_handlers import DefaultBackoffStrategy, ErrorResolution, ResponseAction
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, RequestBodyException, UserDefinedBackoffException
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from requests_cache import CachedRequest


def test_http_client():
    return HttpClient(name="StubHttpClient", logger=MagicMock())

def test_cache_http_client():
    return HttpClient(name="StubCacheHttpClient", logger=MagicMock(), use_cache=True)



def test_cache_filename():
    http_client = test_http_client()
    http_client.cache_filename == f"{http_client._name}.sqlite"

@pytest.mark.parametrize(
    "use_cache, expected_session",
    [
        (True, CachedLimiterSession),
        (False, LimiterSession),
    ],
)
def test_request_session_returns_valid_session(use_cache, expected_session):
    http_client = HttpClient(name="test", logger=MagicMock(), use_cache=use_cache)
    assert isinstance(http_client._request_session(), expected_session)

@pytest.mark.parametrize(
    "deduplicate_query_params, url, params, expected_url",
    [
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            {},
            "https://test_base_url.com/v1/endpoint?param1=value1", id="test_params_only_in_path"
        ),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint",
            {"param1": "value1"},
            "https://test_base_url.com/v1/endpoint?param1=value1", id="test_params_only_in_path"
        ),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint",
            None,
            "https://test_base_url.com/v1/endpoint", id="test_params_is_none_and_no_params_in_path"),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            None,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            id="test_params_is_none_and_no_params_in_path",
        ),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            {"param2": "value2"},
            "https://test_base_url.com/v1/endpoint?param1=value1&param2=value2",
            id="test_no_duplicate_params",
        ),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            {"param1": "value1"},
            "https://test_base_url.com/v1/endpoint?param1=value1",
            id="test_duplicate_params_same_value",
        ),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint?param1=1",
            {"param1": 1},
            "https://test_base_url.com/v1/endpoint?param1=1",
            id="test_duplicate_params_same_value_not_string",
        ),
        pytest.param(
            True,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            {"param1": "value2"},
            "https://test_base_url.com/v1/endpoint?param1=value1&param1=value2",
            id="test_duplicate_params_different_value",
        ),
        pytest.param(
            False,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            {"param1": "value2"},
            "https://test_base_url.com/v1/endpoint?param1=value1&param1=value2",
            id="test_same_params_different_value_no_deduplication",
        ),
        pytest.param(
            False,
            "https://test_base_url.com/v1/endpoint?param1=value1",
            {"param1": "value1"},
            "https://test_base_url.com/v1/endpoint?param1=value1&param1=value1",
            id="test_same_params_same_value_no_deduplication",
        ),
    ],
)
def test_duplicate_request_params_are_deduped(deduplicate_query_params, url, params, expected_url):
    http_client = test_http_client()

    if expected_url is None:
        with pytest.raises(ValueError):
            http_client._create_prepared_request(http_method="get", url=url, dedupe_query_params=deduplicate_query_params, params=params)
    else:
        prepared_request = http_client._create_prepared_request(http_method="get", url=url, dedupe_query_params=deduplicate_query_params, params=params)
        assert prepared_request.url == expected_url

def test_create_prepared_response_given_given_both_json_and_data_raises_request_body_exception():
    http_client = test_http_client()

    with pytest.raises(RequestBodyException):
        http_client._create_prepared_request(http_method="get", url="https://test_base_url.com/v1/endpoint", json={"test": "json"}, data={"test": "data"})

@pytest.mark.parametrize(
    "json, data",
    [
        ({"test": "json"}, None),
        (None, {"test": "data"}),
    ],
)
def test_create_prepared_response_given_either_json_or_data_returns_valid_request(json, data):
    http_client = test_http_client()
    prepared_request = http_client._create_prepared_request(http_method="get", url="https://test_base_url.com/v1/endpoint", json=json, data=data)
    assert prepared_request
    assert isinstance(prepared_request, requests.PreparedRequest)


def test_connection_pool():
    http_client = HttpClient(name="test", logger=MagicMock(), authenticator=TokenAuthenticator("test-token"))
    assert http_client._session.adapters["https://"]._pool_connections == 20

def test_valid_basic_send_request(mocker):
    http_client = test_http_client()
    prepared_request = http_client._create_prepared_request(http_method="get", url="https://test_base_url.com/v1/endpoint")
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"test": "response"}'

    mocker.patch.object(http_client, "_create_prepared_request", return_value=prepared_request)
    mocker.patch.object(requests.Session, "send", return_value=response)

    returned_request, returned_response = http_client.send_request(http_method="get", url="https://test_base_url.com/v1/endpoint", request_kwargs={})

    assert returned_request == prepared_request
    assert returned_response == response

def test_send_raises_airbyte_traced_exception_with_fail_response_action(mocker):
    http_client = test_http_client()
    prepared_request = http_client._create_prepared_request(http_method="get", url="https://test_base_url.com/v1/endpoint")
    response = requests.Response()
    response.status_code = 400

    mocker.patch.object(requests.Session, "send", return_value=response)
    mocker.patch.object(http_client._error_handler, "interpret_response", return_value=ErrorResolution(ResponseAction.FAIL, FailureType.system_error, "test error message"))

    with pytest.raises(AirbyteTracedException):
        http_client._send(prepared_request, {})

def test_send_ignores_with_ignore_reponse_action_and_returns_response(mocker):
    http_client = test_http_client()
    prepared_request = http_client._create_prepared_request(http_method="get", url="https://test_base_url.com/v1/endpoint")
    response = requests.Response()
    response.status_code = 300
    response._content = b'{"test": "response"}'
    http_client._logger.info = MagicMock()

    mocker.patch.object(requests.Session, "send", return_value=response)
    mocker.patch.object(http_client._error_handler, "interpret_response", return_value=ErrorResolution(ResponseAction.IGNORE, FailureType.system_error, "test ignore message"))

    returned_response = http_client._send(prepared_request, {})

    http_client._logger.info.assert_called_once()
    assert returned_response == response

@pytest.mark.parametrize(
        "backoff_time_value, exception_type",
        [
            (0.1, UserDefinedBackoffException),
            (None, DefaultBackoffException)
        ]
)
def test_raises_backoff_exception_with_retry_response_action(mocker, backoff_time_value, exception_type):
    http_client = test_http_client()
    prepared_request = http_client._create_prepared_request(http_method="get", url="https://test_base_url.com/v1/endpoint")
    response = requests.Response()
    response.status_code = 500
    response._content = b'{"test": "response"}'
    http_client._logger.info = MagicMock()

    mocker.patch.object(http_client._backoff_strategy, "backoff_time", return_value=backoff_time_value)
    mocker.patch.object(requests.Session, "send", return_value=response)
    mocker.patch.object(http_client._error_handler, "interpret_response", return_value=ErrorResolution(ResponseAction.RETRY, FailureType.system_error, "test retry message"))

    with pytest.raises(exception_type):
        http_client._send(prepared_request, {})

@pytest.mark.parametrize(
        "backoff_time_value, exception_type",
        [
            (0.1, UserDefinedBackoffException),
            (None, DefaultBackoffException)
        ]
)
def test_raises_backoff_exception_with_response_with_unmapped_error(mocker, backoff_time_value, exception_type):
    http_client = test_http_client()
    prepared_request = requests.PreparedRequest()
    mocked_response = MagicMock(spec=requests.Response)
    mocked_response.status_code = 508
    mocked_response.headers = {}
    error_resolution = ErrorResolution(ResponseAction.RETRY, FailureType.system_error, "test retry message")

    mocker.patch.object(http_client._error_handler, "interpret_response", return_value=error_resolution)

    mocker.patch.object(http_client._backoff_strategy, "backoff_time", return_value=backoff_time_value)
    mocker.patch.object(requests.Session, "send", return_value=mocked_response)

    with pytest.raises(exception_type):
        http_client._send(prepared_request, {})

def test_send_request_given_retry_response_action_retries_and_returns_valid_response(mocker):

    http_client = test_http_client()
    http_method = "get"
    url = "https://test_base_url.com/v1/endpoint"
    valid_response = MagicMock(spec=requests.Response)
    valid_response.status_code = 200
    valid_response.ok = True
    valid_response.headers = {}
    call_count = 2

    def update_test_response_action(*args, **kwargs):
        if http_client._session.send.call_count == call_count:
            return valid_response
        else:
            retry_response = MagicMock(spec=requests.Response)
            retry_response.ok = False
            retry_response.status_code = 500
            retry_response.headers = {}
            return retry_response

    prepared_request = requests.PreparedRequest()

    mocker.patch.object(http_client._backoff_strategy, "backoff_time", return_value=0.123)
    mocker.patch.object(http_client._session, 'send', side_effect=update_test_response_action)

    returned_response = http_client._send_with_retry(prepared_request, request_kwargs={})

    assert http_client._session.send.call_count == call_count
    assert returned_response == valid_response

def test_session_request_exception_raises_backoff_exception():
    http_client = test_http_client()
    http_method = "get"
    url = "https://test_base_url.com/v1/endpoint"
    prepared_request = http_client._create_prepared_request(http_method=http_method, url=url)

    with patch.object(http_client._session, "send", side_effect=requests.exceptions.RequestException):
        with pytest.raises(DefaultBackoffException):
            http_client._send(prepared_request, {})

def test_that_response_was_cached(requests_mock):
    cached_http_client = test_cache_http_client()

    assert isinstance(cached_http_client._session, CachedLimiterSession)

    cached_http_client._session.cache.clear()

    prepared_request = cached_http_client._create_prepared_request(http_method="GET", url="https://google.com/")

    requests_mock.register_uri("GET", "https://google.com/", json='{"test": "response"}')

    cached_http_client._send(prepared_request, {})

    assert requests_mock.called
    requests_mock.reset_mock()

    second_response = cached_http_client._send(prepared_request, {})

    assert isinstance(second_response.request, CachedRequest)
    assert not requests_mock.called
