"""
Utility methods for interacting with AWS CloudWatch Logs
--------------------------------------------------------
"""

from typing import Generator
from typing import Optional

from mypy_boto3 import logs

# The number of seconds to wait to poll for a job's status
DEFAULT_POLLING_INTERVAL: int = 5


def get_log_events(
    logs_client: logs.Client,
    log_group_name: str,
    log_stream_name: str,
    next_token: Optional[str] = None,
) -> Generator[str, None, None]:
    """Gets the latest CloudWatch events.

    Args:
        logs_client: the bot3 AWS (CloudWatch) logs client
        log_group_name: the name of the log group
        log_stream_name: the name of the log stream
    """

    while True:
        if next_token is None:
            response = logs_client.get_log_events(
                logGroupName=log_group_name, logStreamName=log_stream_name, startFromHead=True,
            )
        else:
            response = logs_client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                nextToken=next_token,
                startFromHead=True,
            )
        for event in response["events"]:
            string: str = f"""{event["timestamp"]} {event["message"]}"""
            yield string
        old_next_token = next_token
        next_token = response["nextForwardToken"]
        if next_token == old_next_token:
            break
