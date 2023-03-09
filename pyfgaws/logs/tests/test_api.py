"""Tests for :module:`~pyfgaws.logs.api`"""

from typing import List

import pytest
from mypy_boto3_logs import Client
from mypy_boto3_logs.type_defs import GetLogEventsResponseTypeDef  # noqa

from pyfgaws.logs import Log
from pyfgaws.tests import stubbed_client
from pyfgaws.tests import response_metadata


def stubbed_client_get_log_events(service_responses: List[GetLogEventsResponseTypeDef]) -> Client:
    return stubbed_client(
        service_name="logs", method="get_log_events", service_responses=service_responses
    )


def valid_empty_service_response() -> GetLogEventsResponseTypeDef:
    return {
        "events": [],
        "nextForwardToken": "token-2",
        "nextBackwardToken": "token-2",
        "ResponseMetadata": response_metadata(),
    }


def valid_single_event_service_response() -> GetLogEventsResponseTypeDef:
    return {
        "events": [{"timestamp": 123, "message": "message", "ingestionTime": 123}],
        "nextForwardToken": "token-2",
        "nextBackwardToken": "token-1",
        "ResponseMetadata": response_metadata(),
    }


def valid_multiple_event_service_response() -> GetLogEventsResponseTypeDef:
    return {
        "events": [
            {"timestamp": 1, "message": "message-1", "ingestionTime": 11},
            {"timestamp": 2, "message": "message-2", "ingestionTime": 12},
        ],
        "nextForwardToken": "token-2",
        "nextBackwardToken": "token-1",
        "ResponseMetadata": response_metadata(),
    }


def valid_multiple_service_responses() -> List[GetLogEventsResponseTypeDef]:
    return [
        {
            "events": [{"timestamp": 123, "message": "message", "ingestionTime": 123}],
            "nextForwardToken": "token-2",
            "nextBackwardToken": "token-1",
            "ResponseMetadata": response_metadata(),
        },
        {
            "events": [
                {"timestamp": 1, "message": "message-1", "ingestionTime": 11},
                {"timestamp": 2, "message": "message-2", "ingestionTime": 12},
            ],
            "nextForwardToken": "token-3",
            "nextBackwardToken": "token-2",
            "ResponseMetadata": response_metadata(),
        },
        {
            "events": [],
            "nextForwardToken": "token-3",
            "nextBackwardToken": "token-3",
            "ResponseMetadata": response_metadata(),
        },
    ]


@pytest.mark.parametrize(
    "service_responses",
    [
        [valid_empty_service_response(), valid_empty_service_response()],
        [valid_single_event_service_response(), valid_empty_service_response()],
        [valid_multiple_event_service_response(), valid_empty_service_response()],
        valid_multiple_service_responses(),
    ],
)
def test_get_log_events(service_responses: List[GetLogEventsResponseTypeDef]) -> None:
    client = stubbed_client_get_log_events(service_responses=service_responses)

    events = list(Log(client=client, group="name", stream="name"))

    expected = [
        f"""{item["timestamp"]} {item["message"]}"""
        for service_response in service_responses
        for item in service_response["events"]
    ]

    assert events == expected
