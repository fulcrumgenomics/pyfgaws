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
import mypy_boto3_batch as batch
import mypy_boto3_logs as logs
from mypy_boto3_batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3_batch.type_defs import KeyValuePairTypeDef  # noqa
from mypy_boto3_batch.type_defs import SubmitJobResponseTypeDef  # noqa

from pyfgaws.batch import BatchJob
from pyfgaws.batch.api import list_jobs
from pyfgaws.batch import Status
from pyfgaws.logs import DEFAULT_POLLING_INTERVAL as DEFAULT_LOGS_POLLING_INTERVAL
from pyfgaws.logs import Log
from fgpyo.util.string import column_it


def _log_it(
    region_name: str, job: BatchJob, logger: logging.Logger, delay: Optional[int] = None
) -> None:
    """Creates a background thread to print out CloudWatch logs.

    Args:
        region_name: the AWS region
        job: the AWS batch job
        logger: the logger to which logs should be printed
        delay: the number of seconds to wait after polling for status.  Only used when
            `--watch-until` is `true`.
    """
    if job.stream is None:
        return None
    # Create a background thread
    logs_thread = threading.Thread(
        target=_watch_logs, args=(region_name, job, logger, delay), daemon=True
    )
    logs_thread.start()


def watch_job(
    *,
    job_id: str,
    region_name: Optional[str] = None,
    print_logs: bool = True,
    delay: Optional[int] = None,
) -> None:
    """Watches an AWS batch job.

    This tool a small random jitter (+/-2 seconds) to the delay to help avoid AWS batch API
    limits for monitoring batch jobs in the cases of many requests across concurrent jobs.  A
    minimum delay of 1 second is subsequently applied.

    Args:
        job_id: the AWS batch job identifier
        region_name: the AWS region
        print_logs: true to print CloudWatch logs, false otherwise
        delay: the number of seconds to wait after polling for status.  Only used when
            `--watch-until` is `true`.
    """
    logger = logging.getLogger(__name__)

    client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )

    # Create the job
    job: BatchJob = BatchJob.from_id(client=client, job_id=job_id)

    logger.info(f"Watching job with name '{job.name}' and id '{job.job_id}'")
    if print_logs:
        _log_it(region_name=region_name, job=job, logger=logger, delay=delay)
        if delay is None:
            time.sleep(DEFAULT_LOGS_POLLING_INTERVAL)
        else:
            time.sleep(delay)

    job.wait_on_complete(delay=delay)
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
    delay: Optional[int] = None,
) -> None:
    """Submits a batch job and optionally waits for it to reach one of the given states.

    This tool a small random jitter (+/-2 seconds) to the delay to help avoid AWS batch API
    limits for monitoring batch jobs in the cases of many requests across concurrent jobs.  A
    minimum delay of 1 second is subsequently applied.

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
        delay: the number of seconds to wait after polling for status.  Only used when
            `--watch-until` is `true`.
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
            _log_it(region_name=region_name, job=job, logger=logger, delay=delay)

        # Wait for the job to reach on of the statuses
        job.wait_on(
            status_to_state=dict((status, True) for status in watch_until),
            after_success=after_success,
            delay=delay,
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
    delay: Optional[int] = None,
    polling_interval: int = DEFAULT_LOGS_POLLING_INTERVAL,
    indefinitely: bool = True,
) -> None:
    """A method to watch logs.

    Args:
        region_name: the AWS region
        job: the AWS batch job
        logger: the logger to which logs should be printed
        delay: the number of seconds to wait after polling for status.  Only used when
            `--watch-until` is `true`.
        polling_interval: the default time to wait for new CloudWatch logs after no more logs are
            returned
        indefinitely: true to watch indefinitely, false to print only the available logs
    """
    # wait until it's running to get the CloudWatch logs
    job.wait_on_running(delay=delay)

    client: Optional[logs.Client] = boto3.client(
        service_name="logs", region_name=region_name  # type: ignore
    )
    log: Log = Log(client=client, group=job.group, stream=job.stream)

    try:
        while True:
            try:
                for line in log:
                    logger.info(line)
            except client.exceptions.ResourceNotFoundException:
                logger.warning("The log stream has not been created, will try again.")
            time.sleep(polling_interval)
            log.reset()
            if not indefinitely:
                break
    except Exception as ex:
        logger.error(f"Encountered an exception while watching logs: {ex}")
        raise ex


def monitor(
    *,
    job_ids: Optional[List[str]] = None,
    queue: Optional[str] = None,
    region_name: Optional[str] = None,
    delay: Optional[int] = None,
    per_job: bool = False,
    monitor_queue: bool = False,
) -> None:
    """Monitor the Batch jobs with given job identifiers or job queue.

    If job identifiers are given, then this tool will exit once all jobs have reached a terminal
    state.  If a job queue is given, then this tool will keep polling for new jobs as long as there
    are known jobs that are not in a terminal state.  In other words, exit once all known jobs have
    completed, and if a queue is given, keep checking to see if any new jobs show up.

    Args:
        job_ids: the AWS batch job identifier(s)
        queue: the name of the AWS batch queue
        region_name: the AWS region
        delay: the number of seconds to wait after polling for status(es).
        per_job: true to monitor per-job status information, otherwise jobs will be summarized by
                  status
        monitor_queue: never exit and keep monitoring for new jobs.  Can only be used with the
                       `--queue` option
    """
    logger = logging.getLogger(__name__)
    assert (
        job_ids is not None or queue is not None
    ), "Either --job-ids or --queue must be specified"
    assert job_ids is None or queue is None, "Both --job-ids or --queue cannot be specified"
    assert not monitor_queue or queue is not None, "--queue must be used with --monitor-queue"

    client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )

    # get the list of batch jobs from job ids.  If using a queue, the job ids will be retrieved
    # later
    jobs: List[BatchJob] = []
    if job_ids is not None:
        jobs = [BatchJob.from_id(client=client, job_id=job_id) for job_id in job_ids]
        logger.info(f"Monitoring {len(job_ids):,d} jobs.")
    else:
        logger.info(f"Monitoring jobs for queue: {queue}")

    while True:
        print("\033[H")  # clear the screen!

        # if monitoring a queue, get the full list of jobs
        if queue is not None:
            logger.info(f"Retrieving job ids for queue: {queue}")
            # add newly seen jobs
            for job_id in list_jobs(client=client, queue=queue):
                if all(job_id != job.job_id for job in jobs):
                    jobs.append(BatchJob.from_id(client=client, job_id=job_id))

        # print out detailed stats, and update status counts
        status_counts: Dict[Optional[Status], int] = {status: 0 for status in Status}
        status_counts[None] = 0
        table_detail: List[List[str]] = [["JOB_NAME", "JOB_ID", "STATUS"]]
        job: BatchJob
        num_done: int = 0
        for job in jobs:
            status: Optional[Status] = job.get_status()
            status_counts[status] = status_counts[status] + 1
            status_name = "Unknown" if status is None else status.name
            if per_job:
                table_detail.append([job.name, job.job_id, status_name])
            if status is not None and (status == Status.Succeeded or status == Status.Failed):
                num_done += 1
        print("\033[2J\033[H")  # clear the screen!
        if per_job:
            print(column_it(table_detail, delimiter="  "))

        # we are done polling if we're not monitoring a queue and all the known jobs are complete
        all_done = not monitor_queue and num_done == len(jobs)

        # print out summary stats
        if all_done or not per_job:
            table_summary: List[List[str]] = [["STATUS", "COUNT"]]
            for status, count in status_counts.items():
                status_name = "Unknown" if status is None else status.name
                table_summary.append([status_name, f"{count:,d}"])
            print("\033[2J\033[H")  # clear the screen!
            print(column_it(table_summary, delimiter="  "))

        if all_done:
            break

        if delay is None:
            time.sleep(DEFAULT_LOGS_POLLING_INTERVAL)
        else:
            time.sleep(delay)

    logger.info("All jobs completed")
