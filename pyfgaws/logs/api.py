"""
Utility methods for interacting with AWS CloudWatch Logs
--------------------------------------------------------
"""

import queue
from typing import Iterator
from typing import Optional

from mypy_boto3 import logs

# The number of seconds to wait to poll for a job's status
DEFAULT_POLLING_INTERVAL: int = 5


class Log(Iterator[str]):
    """Log to iterate through CloudWatch logs.

    Iterating over this class returns all available events.

    The `reset()` method allows iterating across any newly available events since the last
    iteration.

    Attributes:
        client: the logs client
        group: the log group name
        stream: the log stream name
        next_token: the token for the next set of items to return, or None if the oldest events.
    """

    def __init__(
        self, client: logs.Client, group: str, stream: str, next_token: Optional[str] = None
    ) -> None:
        self.client: logs.Client = client
        self.group: str = group
        self.stream: str = stream
        self.next_token: Optional[str] = next_token
        self._events: queue.Queue = queue.Queue()
        self._is_done: bool = False

    def __next__(self) -> str:
        if not self._events.empty():
            return self._events.get()
        elif self._is_done:
            raise StopIteration

        # add more events
        if self.next_token is None:
            response = self.client.get_log_events(
                logGroupName=self.group, logStreamName=self.stream, startFromHead=True,
            )
        else:
            response = self.client.get_log_events(
                logGroupName=self.group,
                logStreamName=self.stream,
                nextToken=self.next_token,
                startFromHead=True,
            )
        for event in response["events"]:
            string: str = f"""{event["timestamp"]} {event["message"]}"""
            self._events.put(item=string)
        old_next_token = self.next_token
        self.next_token = response["nextForwardToken"]
        self._is_done = self.next_token == old_next_token

        return self.__next__()

    def reset(self) -> "Log":
        """Reset the iterator such that new events may be returned."""
        self._is_done = False
        return self
