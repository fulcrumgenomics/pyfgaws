"""
Command-line tools for interacting with AWS CloudWatch Logs
-----------------------------------------------------------
"""

import logging
import time
from typing import Callable
from typing import List
from typing import Optional

import boto3

from pyfgaws.logs import DEFAULT_POLLING_INTERVAL
from pyfgaws.logs import get_log_events


def watch_logs(
    *,
    log_group_name: str,
    log_stream_name: str,
    region_name: Optional[str] = None,
    polling_interval: int = DEFAULT_POLLING_INTERVAL,
    raw_output: bool = False,
) -> None:
    """Watches a cloud watch log stream

    Args:
        log_group_name: the name of the log group
        log_stream_name: the name of the log stream
        region_name: the AWS region
    """
    logger = logging.getLogger(__name__)

    logs_client = boto3.client(
        service_name="logs", region_name=region_name  # type: ignore
    )

    logger.info(f"Polling log group '{log_group_name}' and stream '{log_stream_name}'")
    while True:
        for line in get_log_events(
            logs_client=logs_client,
            log_group_name=log_group_name,
            log_stream_name=log_stream_name,
        ):
            if raw_output:
                print(line)
            else:
                logger.info(line)
        time.sleep(polling_interval)


# The AWS CloudWatch log tools to expose on the command line
LOGS_TOOLS: List[Callable] = [watch_logs]
