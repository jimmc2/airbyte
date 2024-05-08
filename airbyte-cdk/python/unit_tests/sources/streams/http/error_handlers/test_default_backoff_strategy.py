# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import requests
from typing import Optional, Union
import pytest

from airbyte_cdk.sources.streams.http.error_handlers import BackoffStrategy, DefaultBackoffStrategy



def test_given_no_arguments_default_backoff_strategy_returns_default_values():
    response = requests.Response()
    backoff_strategy = DefaultBackoffStrategy()
    assert backoff_strategy.max_retries == 5
    assert backoff_strategy.max_time == 600
    assert backoff_strategy.retry_factor == 5
    assert backoff_strategy.backoff_time(response) == None

class CustomBackoffStrategy(BackoffStrategy):

    def __init__(self, max_retries: int = 5, max_time: int = 60 * 10, retry_factor: float = 5):
        self.max_retries = max_retries
        self.max_time = max_time
        self.retry_factor = retry_factor

    def backoff_time(self, response_or_exception: Optional[Union[requests.Response, requests.RequestException]]) -> Optional[float]:
        return response_or_exception.headers["Retry-After"]


def test_given_valid_arguments_default_backoff_strategy_returns_values():


    response = requests.Response()
    response.headers["Retry-After"] = 123
    backoff_strategy = CustomBackoffStrategy()
    assert backoff_strategy.max_retries == 5
    assert backoff_strategy.max_time == 600
    assert backoff_strategy.retry_factor == 5
    assert backoff_strategy.backoff_time(response) == 123
