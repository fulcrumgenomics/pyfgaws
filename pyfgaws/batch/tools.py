"""
Command-line tools for interacting with AWS Batch
-------------------------------------------------
"""

import logging
from pathlib import Path
from typing import Callable
from typing import List
from typing import Optional

import boto3
from mypy_boto3 import batch
from mypy_boto3 import logs
from mypy_boto3.batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa

from pyfgaws.batch import DEFAULT_POLLING_INTERVAL
from pyfgaws.batch import submit_job
from pyfgaws.batch import wait_for_job


def watch_job(
    *,
    job_id: str,
    region_name: Optional[str] = None,
    polling_interval: int = DEFAULT_POLLING_INTERVAL,
    print_cloudwatch_logs: bool = True,
) -> None:
    """Watches an AWS batch job.

    Args:
        job_id: the AWS batch job identifier
        region_name: the AWS region
        polling_interval: the number of seconds to wait after polling for a jobs status to try
            again
        print_cloudwatch_logs: true to print CloudWatch logs, false otherwise
    """
    logger = logging.getLogger(__name__)

    batch_client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )
    logs_client: Optional[logs.Client] = None

    if print_cloudwatch_logs:
        logs_client = boto3.client(
            service_name="logs", region_name=region_name  # type: ignore
        )

    wait_for_job(
        batch_client=batch_client,
        job_id=job_id,
        logs_client=logs_client,
        polling_interval=polling_interval,
        logger=logger,
    )


def run_job(
    *,
    template_json: Path,
    job_definition: str,
    region_name: Optional[str] = None,
    polling_interval: int = DEFAULT_POLLING_INTERVAL,
    print_cloudwatch_logs: bool = True,
    queue: Optional[str] = None,
    cpus: Optional[int] = None,
    mem_mb: Optional[int] = None,
    command: Optional[str] = None,
) -> None:
    """Submits a batch job and waits for it to complete.

    Args:
        template_json: path to a JSON job template
        job_definition: the ARN for the AWS batch job definition, or the name of the job definition
            to get the latest revision
        region_name: the AWS region
        polling_interval: the number of seconds to wait after polling for a jobs status to try
            again
        print_cloudwatch_logs: true to print CloudWatch logs, false otherwise
        queue: the name of the AWS bathc queue
        cpus: the number of CPUs to request
        mem_mb: the amount of memory to request (in megabytes)
        command: the command to use
    """
    logger = logging.getLogger(__name__)

    batch_client: batch.Client = boto3.client(
        service_name="batch", region_name=region_name  # type: ignore
    )
    logs_client: Optional[logs.Client] = None

    if print_cloudwatch_logs:
        logs_client = boto3.client(
            service_name="logs", region_name=region_name  # type: ignore
        )

    logger.info("Submitting job...")
    job = submit_job(
        batch_client=batch_client,
        template_json=template_json,
        job_definition=job_definition,
        queue=queue,
        cpus=cpus,
        mem_mb=mem_mb,
        command=command,
        logger=logger,
    )
    logger.info(f"""Job submitted with name '{job["jobName"]}' and id '{job["jobId"]}'""")

    wait_for_job(
        batch_client=batch_client,
        job_id=job["jobId"],
        logs_client=logs_client,
        polling_interval=polling_interval,
        logger=logger,
    )


# The AWS Batch tools to expose on the command line
BATCH_TOOLS: List[Callable] = [run_job, watch_job]
