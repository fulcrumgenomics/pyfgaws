"""
Command-line tools for interacting with AWS Batch
-------------------------------------------------
"""

import logging
import threading
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import boto3
from mypy_boto3 import batch
from mypy_boto3 import logs
from mypy_boto3.batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import KeyValuePairTypeDef  # noqa

from pyfgaws.batch import BatchJob
from pyfgaws.logs import DEFAULT_POLLING_INTERVAL as DEFAULT_LOGS_POLLING_INTERVAL
from pyfgaws.logs import Log


def _log_it(region_name: str, job: BatchJob, logger: logging.Logger) -> None:
    """Creates a background thread to print out CloudWatch logs.

    Args:
        region_name: the AWS region
        job: the AWS batch job
        logger: the logger to which logs should be printed
    """
    # Create a background thread
    logs_thread = threading.Thread(
        target=_watch_logs, args=(region_name, job, logger), daemon=True
    )
    logs_thread.start()


def watch_job(*, job_id: str, region_name: Optional[str] = None, print_logs: bool = True,) -> None:
    """Watches an AWS batch job.

    Args:
        job_id: the AWS batch job identifier
        region_name: the AWS region
        print_logs: true to print CloudWatch logs, false otherwise
    """
    logger = logging.getLogger(__name__)

    client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )

    # Create the job
    job: BatchJob = BatchJob.from_id(client=client, job_id=job_id)

    logger.info(f"Watching job with name '{job.name}' and id '{job.job_id}'")
    if print_logs:
        _log_it(region_name=region_name, job=job, logger=logger)

    job.wait_on_complete()
    logger.info(
        f"Job completed with name '{job.name}', id '{job.job_id}', and status '{job.get_status()}'"
    )


def run_job(
    *,
    name: str,
    job_definition: str,
    region_name: Optional[str] = None,
    print_logs: bool = True,
    queue: Optional[str] = None,
    cpus: Optional[int] = None,
    mem_mb: Optional[int] = None,
    command: List[str] = [],
    parameters: Optional[Dict[str, Any]] = None,
    environment: Optional[KeyValuePairTypeDef] = None,
    watch: bool = False,
) -> None:
    """Submits a batch job and optionally waits for it to complete.

    Args:
        name: then name of the batch job
        job_definition: the ARN for the AWS batch job definition, or the name of the job definition
            to get the latest revision
        region_name: the AWS region
        print_logs: true to print CloudWatch logs, false otherwise
        queue: the name of the AWS batch queue
        cpus: the number of CPUs to request
        mem_mb: the amount of memory to request (in megabytes)
        command: the command(s) to use
        parameters: the (JSON) dictionary of parameters to use
        environment: the (JSON) dictionary of environment variables to use
    """
    logger = logging.getLogger(__name__)

    batch_client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )

    job = BatchJob(
        client=batch_client,
        name=name,
        queue=queue,
        job_definition=job_definition,
        cpus=cpus,
        mem_mb=mem_mb,
        command=command,
        environment=None if environment is None else [environment],
        parameters=parameters,
        logger=logger,
    )

    # Submit the job
    logger.info("Submitting job...")
    job.submit()
    logger.info(f"Job submitted with name '{job.name}' and id '{job.job_id}'")

    # Optionally wait on it to complete
    if watch:
        if print_logs:
            _log_it(region_name=region_name, job=job, logger=logger)

        # Wait for the job to complete
        job.wait_on_complete()
        logger.info(
            f"Job completed with name '{job.name}', id '{job.job_id}'"
            f", and status '{job.get_status()}'"
        )


def _watch_logs(
    region_name: str,
    job: BatchJob,
    logger: logging.Logger,
    polling_interval: int = DEFAULT_LOGS_POLLING_INTERVAL,
) -> None:
    """A method to watch logs indefinitely.

    Args:
        region_name: the AWS region
        job: the AWS batch job
        logger: the logger to which logs should be printed
        polling_interval: the default time to wait for new CloudWatch logs after no more logs are
            returned
    """
    # wait until it's running to get the CloudWatch logs
    job.wait_on_running()

    client: Optional[logs.Client] = boto3.client(
        service_name="logs", region_name=region_name  # type: ignore
    )
    log: Log = Log(client=client, group="/aws/batch/job", stream=job.stream)

    while True:
        for line in log:
            logger.info(line)
        time.sleep(polling_interval)
        log.reset()
