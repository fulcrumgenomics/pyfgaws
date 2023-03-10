"""
Command-line tools for interacting with AWS CloudWatch Logs
-----------------------------------------------------------
"""

import logging
import time
from typing import Optional

import boto3

from pyfgaws.logs import DEFAULT_POLLING_INTERVAL
from pyfgaws.logs import Log


def watch_logs(
    *,
    group: str,
    stream: str,
    region_name: Optional[str] = None,
    polling_interval: int = DEFAULT_POLLING_INTERVAL,
    raw_output: bool = False,
) -> None:
    """Watches a cloud watch log stream

    Args:
        group: the name of the log group
        stream: the name of the log stream
        region_name: the AWS region
        polling_interval: the time to wait before polling for new logs
        raw_output: output the logs directly to standard output, otherwise uses the internal
            logger.
    """
    logger = logging.getLogger(__name__)

    client = boto3.client(service_name="logs", region_name=region_name)  # type: ignore

    logger.info(f"Polling log group '{group}' and stream '{stream}'")

    log: Log = Log(client=client, group=group, stream=stream)

    while True:
        for line in log:
            if raw_output:
                print(line)
            else:
                logger.info(line)
        time.sleep(polling_interval)
        log.reset()
