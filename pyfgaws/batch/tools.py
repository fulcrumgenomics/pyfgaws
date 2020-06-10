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
from mypy_boto3.batch.type_defs import KeyValuePairTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa

from pyfgaws.batch import BatchJob
from pyfgaws.batch import Status
from pyfgaws.logs import DEFAULT_POLLING_INTERVAL as DEFAULT_LOGS_POLLING_INTERVAL
from pyfgaws.logs import Log


def _log_it(region_name: str, job: BatchJob, logger: logging.Logger) -> None:
    """Creates a background thread to print out CloudWatch logs.

    Args:
        region_name: the AWS region
        job: the AWS batch job
        logger: the logger to which logs should be printed
    """
    if job.stream is None:
        return None
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
    end_status = job.get_status()
    logger.info(
        f"Job completed with name '{job.name}', id '{job.job_id}', and status '{end_status}'"
    )


def run_job(
    *,
    job_definition: str,
    name: Optional[str] = None,
    region_name: Optional[str] = None,
    print_logs: bool = True,
    queue: Optional[str] = None,
    cpus: Optional[int] = None,
    mem_mb: Optional[int] = None,
    command: List[str] = [],
    parameters: Optional[Dict[str, Any]] = None,
    environment: Optional[KeyValuePairTypeDef] = None,
    watch_until: List[Status] = [],
    after_success: bool = False,
) -> None:
    """Submits a batch job and optionally waits for it to reach one of the given states.

    Args:
        job_definition: the ARN for the AWS batch job definition, or the name of the job definition
            to get the latest revision
        name: the name of the job, otherwise one will be automatically generated
        region_name: the AWS region
        print_logs: true to print CloudWatch logs, false otherwise
        queue: the name of the AWS batch queue
        cpus: the number of CPUs to request
        mem_mb: the amount of memory to request (in megabytes)
        command: the command(s) to use
        parameters: the (JSON) dictionary of parameters to use
        environment: the (JSON) dictionary of environment variables to use
        watch_until: watch until any of the given statuses are reached.  If the job reaches a
            status past all statuses, then an exception is thrown.  For example, `Running` will
            fail if `Succeeded` is reached, while `Succeeded` will fail if `Failed` is reached.  To
            wait for the job to complete regardless of status, use both `Succeeded` and `Failed`.
            See the `--after-success` option to control this behavior.
        after_success: true to treat states after the `watch_until` states as success, otherwise
            failure.
    """
    logger = logging.getLogger(__name__)

    batch_client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )

    job = BatchJob(
        client=batch_client,
        queue=queue,
        job_definition=job_definition,
        name=name,
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
    # Note: watch_until should be type Optional[List[Status]], but see:
    # - https://github.com/anntzer/defopt/issues/83
    if len(watch_until) > 0:
        if print_logs:
            _log_it(region_name=region_name, job=job, logger=logger)

        # Wait for the job to reach on of the statuses
        job.wait_on(
            status_to_state=dict((status, True) for status in watch_until),
            after_success=after_success,
        )
        end_status: Status = job.get_status()

        if print_logs and end_status.logs:
            _watch_logs(region_name=region_name, job=job, logger=logger, indefinitely=False)

        logger.info(
            f"Job name '{job.name}' and id '{job.job_id}' reached status '" f"{end_status.status}'"
        )


def _watch_logs(
    region_name: str,
    job: BatchJob,
    logger: logging.Logger,
    polling_interval: int = DEFAULT_LOGS_POLLING_INTERVAL,
    indefinitely: bool = True,
) -> None:
    """A method to watch logs.

    Args:
        region_name: the AWS region
        job: the AWS batch job
        logger: the logger to which logs should be printed
        polling_interval: the default time to wait for new CloudWatch logs after no more logs are
            returned
        indefinitely: true to watch indefinitely, false to print only the available logs
    """
    # wait until it's running to get the CloudWatch logs
    job.wait_on_running()

    client: Optional[logs.Client] = boto3.client(
        service_name="logs", region_name=region_name  # type: ignore
    )
    log: Log = Log(client=client, group="/aws/batch/job", stream=job.stream)

    try:
        while True:
            for line in log:
                logger.info(line)
            time.sleep(polling_interval)
            log.reset()
            if not indefinitely:
                break
    except Exception as ex:
        logger.error(f"Encountered an exception while watching logs: {ex}")
        raise ex
