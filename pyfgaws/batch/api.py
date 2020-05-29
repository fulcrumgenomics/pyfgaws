"""
Utility methods for interacting with AWS Batch
----------------------------------------------
"""
import json
import logging
import time
from pathlib import Path
from typing import Optional

from mypy_boto3 import batch
from mypy_boto3 import logs
from mypy_boto3.batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa

from pyfgaws.logs import get_log_events

# The number of seconds to wait to poll for a job's status
DEFAULT_POLLING_INTERVAL: int = 30


def submit_job(
    batch_client: batch.Client,
    template_json: Path,
    job_definition: str,
    queue: Optional[str] = None,
    cpus: Optional[int] = None,
    mem_mb: Optional[int] = None,
    command: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> batch.type_defs.SubmitJobResponseTypeDef:
    """Submits a batch job.

    Args:
        batch_client: the boto3 AWS batch client
        template_json: path to a JSON job template
        job_definition: the ARN for the AWS batch job definition, or the name of the job definition
            to get the latest revision
        queue: the name of the AWS batch queue
        cpus: the number of CPUs to request
        mem_mb: the amount of memory to request (in megabytes)
        command: the command to use
        logger: logger to write status messages

    Returns:
        the job response
    """

    # Get the latest job definition ARN if not given
    if job_definition.startswith("arn:aws:batch:"):
        if logger is not None:
            logger.info(f"Using provided job definition '{job_definition}'")
        job_definition_arn = job_definition
    else:
        if logger is not None:
            logger.info(f"Retrieving the latest job definition for {job_definition}")
        response = batch_client.describe_job_definitions(jobDefinitionName=job_definition)
        latest = max(response["jobDefinitions"], key=lambda d: d["revision"])
        job_definition_arn = latest["jobDefinitionArn"]
        assert latest["jobDefinitionName"] == job_definition, "Bug"
        if logger is not None:
            logger.info(f"Retrieved latest job definition '{job_definition}'")

    # Load up the batch_client job parameters and override any parameters
    # This will be saved in the EVENT environment variable
    with template_json.open("r") as reader:
        event = json.load(reader)
    batch_job_params = event["batchJob"]
    overrides = batch_job_params["containerOverrides"]
    job_name = batch_job_params["jobName"]
    if queue is not None:
        batch_job_params["jobQueue"] = queue

    # Add container overrides
    # container overrides not implemented
    # - instanceType
    # - environment
    # - resourceRequirements
    # - nodeOverrides
    # - retryStrategy
    # - timeout
    if cpus is not None:
        overrides["vcpus"] = f"{cpus}"
    if mem_mb is not None:
        overrides["memory"] = f"{mem_mb}"
    if command is not None:
        overrides["command"] = command
    event_string: str = json.dumps(event)
    if logger is not None:
        logger.debug(event_string)

    # Build the container overrides
    container_overrides = overrides  # TODO: deep copy
    if "environment" not in container_overrides:
        container_overrides["environment"] = [{"name": "EVENT", "value": event_string}]
    else:
        assert all(
            item.get("name") != "EVENT" for item in container_overrides["environment"]
        ), "Found reserved environment name 'EVENT'"

    # Submit the job
    # Arguments not implemented:
    # - arrayProperties
    # - dependsOn
    # - parameters
    return batch_client.submit_job(
        jobName=job_name,
        jobDefinition=job_definition_arn,
        jobQueue=batch_job_params["jobQueue"],
        containerOverrides=container_overrides,
    )


def wait_for_job(
    batch_client: batch.Client,
    job_id: str,
    logs_client: Optional[logs.Client] = None,
    polling_interval: int = DEFAULT_POLLING_INTERVAL,
    logger: Optional[logging.Logger] = None,
) -> batch.type_defs.DescribeJobsResponseTypeDef:
    """Waits for a AWS batch job to finish.

    Args:
        batch_client: the boto3 AWS batch client
        job_id: the job identifier
        logs_client: the boto3 AWS logs client for retrieving CloudWatch logs. No logging will be
          performed if not given.
        polling_interval: the number of seconds to wait after polling for a jobs status to try
            again
        logger: logger to write status messages

    Returns:
        the describe jobs response
    """

    if logger is not None:
        logger.info(f"Waiting on job to finish: {job_id}")

    while True:
        jobs_response = batch_client.describe_jobs(jobs=[job_id])
        job_statuses = jobs_response["jobs"]
        assert len(job_statuses) == 1
        job_status = job_statuses[0]["status"]
        if logger is not None:
            logger.info(f"Polled job with id '{job_id}' has status '{job_status}'")

        if logs_client is not None:
            log_stream_name = job_statuses[0]["container"]["logStreamName"]
            for line in get_log_events(
                logs_client=logs_client,
                log_group_name="/aws/batch/job",
                log_stream_name=log_stream_name,
            ):
                if logger is None:
                    print(line)
                else:
                    logger.info(line)

        # Check the job status
        if job_status == "SUCCEEDED" or job_status == "FAILED":
            break
        time.sleep(polling_interval)

    return jobs_response
