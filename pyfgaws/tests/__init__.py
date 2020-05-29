"""Testing utilities for :module:`~pyfgaws`"""

from typing import Any
from typing import List

import botocore.session
import pytest
from botocore.stub import Stubber
from mypy_boto3.logs import Client


def _to_name(tool) -> str:  # type: ignore
    """Gives the tool name for a function by taking the function name and replacing
    underscores with hyphens."""
    return tool.__name__.replace("_", "-")


def test_tool_funcs(tool, main) -> None:  # type: ignore
    name = _to_name(tool)
    argv = [name, "-h"]
    with pytest.raises(SystemExit) as e:
        main(argv=argv)
    assert e.type == SystemExit
    assert e.value.code == 0  # code should be 0 for help


def stubbed_client(service_name: str, method: str, service_responses: List[Any]) -> Client:
    """Creates a stubbed client.

    Args:
        service_name: the name of the AWS service (ex. logs, batch) to stub
        method: The name of the client method to stub
        service_responses: one or more service responses to add

    Returns:
        an activated stubbed client with the given responses added
    """

    client = botocore.session.get_session().create_client(service_name)
    stubber = Stubber(client)
    for service_response in service_responses:
        stubber.add_response(method=method, service_response=service_response)
    stubber.activate()
    return client
