"""
Utility methods for interacting with AWS Batch
----------------------------------------------
"""
import copy
import enum
import json
import logging
import sys
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional

import botocore
from botocore.waiter import Waiter as BotoWaiter
from mypy_boto3 import batch
from mypy_boto3.batch.type_defs import ArrayPropertiesTypeDef  # noqa
from mypy_boto3.batch.type_defs import ContainerDetailTypeDef  # noqa
from mypy_boto3.batch.type_defs import ContainerOverridesTypeDef  # noqa
from mypy_boto3.batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import JobDependencyTypeDef  # noqa
from mypy_boto3.batch.type_defs import JobDetailTypeDef  # noqa
from mypy_boto3.batch.type_defs import JobTimeoutTypeDef  # noqa
from mypy_boto3.batch.type_defs import JobTimeoutTypeDef  # noqa
from mypy_boto3.batch.type_defs import KeyValuePairTypeDef  # noqa
from mypy_boto3.batch.type_defs import NodeOverridesTypeDef  # noqa
from mypy_boto3.batch.type_defs import ResourceRequirementTypeDef  # noqa
from mypy_boto3.batch.type_defs import RetryStrategyTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa


@enum.unique
class Status(enum.Enum):
    Submitted = "SUBMITTED"
    Pending = "PENDING"
    Runnable = "RUNNABLE"
    Starting = "STARTING"
    Running = "RUNNING"
    Succeeded = "SUCCEEDED"
    Failed = "FAILED"


def get_latest_job_definition_arn(client: batch.Client, job_definition: str) -> str:
    """Retrieves the latest job definition ARN.

    Args:
        client: the AWS batch client
        job_definition: the AWS batch job definition name

    Returns:
        the latest job definition ARN.
    """
    response = client.describe_job_definitions(jobDefinitionName=job_definition)
    latest = max(response["jobDefinitions"], key=lambda d: d["revision"])
    return latest["jobDefinitionArn"]


class Waiters:
    """A utility class to store waiters for AWS batch status."""

    def __init__(self, client: batch.Client, config: Optional[Dict] = None) -> None:
        """
        Args:
            client: the AWS batch client
            config: an optional configuration dictionary for the Waiters to use, otherwise, uses
                the default waiters in `waiters.json`
        """
        self._client: batch.Client = client
        self._config: Optional[Dict]

        if config is not None:
            self._config = config
        else:
            config_path = Path(__file__).with_name("waiters.json").absolute()
            with open(config_path) as config_file:
                self._default_config = json.load(config_file)
            self._config = copy.deepcopy(self._default_config)

        self._model = botocore.waiter.WaiterModel(self._config)

    def waiters(self) -> List[str]:
        """Returns the list of supported waiters."""
        return self._model.waiter_names

    def get(self, name: str) -> BotoWaiter:
        """Builds and returns the waiter with the given name"""
        return botocore.waiter.create_waiter_with_client(name, self._model, self._client)


class BatchJob:
    """Stores information about a batch job.

    The following arguments override the provided container overrides: `cpus`, `mem_mb` (
    overrides `memory`), `command`, `instance_type`, `environment`, and `resourceRequirements`.

    Attributes:
        client: the batch client to use
        name: the name of the job
        queue: the nae of the AWS batch queue
        job_definition: the ARN for the AWS batch job definition, or the name of the job definition
            to get the latest revision
        cpus: the number of CPUs to request
        mem_mb: the amount of memory to request (in megabytes)
        command: the command to use
        instance_type: the instance type to use
        environment: the environment variables to use
        resource_requirements: a list of resource requirements for the job (type and amount)s
        array_properties: the array properties for this job
        depends_on: the list of jobs to depend on
        parameters: additional parameters passed to the job that replace parameter substitution
            placeholders that are set in the job definition.
        container_overrides: the container overrides that specify the name of a container in the
            specified job definition and the overrides it should receive
        node_overrides: list of node overrides that specify the node range to target and the
            container overrides for that node range.
        retry_strategy: the retry strategy to use for failed jobs from the `submit_job` operation.
        timeout: the timeout configuration
        logger: logger to write status messages
    """

    def __init__(
        self,
        client: batch.Client,
        name: str,
        queue: str,
        job_definition: str,
        cpus: Optional[int] = None,
        mem_mb: Optional[int] = None,
        command: Optional[List[str]] = None,
        instance_type: Optional[str] = None,
        environment: Optional[List[KeyValuePairTypeDef]] = None,
        resource_requirements: Optional[List[ResourceRequirementTypeDef]] = None,
        array_properties: Optional[ArrayPropertiesTypeDef] = None,
        depends_on: Optional[List[JobDependencyTypeDef]] = None,
        parameters: Optional[Dict[str, str]] = None,
        container_overrides: Optional[ContainerOverridesTypeDef] = None,
        node_overrides: Optional[NodeOverridesTypeDef] = None,
        retry_strategy: Optional[RetryStrategyTypeDef] = None,
        timeout: Optional[JobTimeoutTypeDef] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:

        self.client: batch.Client = client

        # Get the latest job definition ARN if not given
        if job_definition.startswith("arn:aws:batch:"):
            if logger is not None:
                logger.info(f"Using provided job definition '{job_definition}'")
            self.job_definition_arn = job_definition
        else:
            if logger is not None:
                logger.info(f"Retrieving the latest job definition for {job_definition}")
            self.job_definition_arn = get_latest_job_definition_arn(
                client=self.client, job_definition=job_definition
            )
            if logger is not None:
                logger.info(f"Retrieved latest job definition '{job_definition}'")

        # Main arguments
        self.name: str = name  # should we just make up a name?
        self.queue: str = queue  # should we just get the first one?
        self.array_properties: ArrayPropertiesTypeDef = (
            {} if array_properties is None else array_properties
        )
        self.depends_on: List[JobDependencyTypeDef] = (
            [] if depends_on is None else copy.deepcopy(depends_on)
        )
        self.parameters = {} if parameters is None else copy.deepcopy(parameters)
        self.container_overrides = (
            {} if container_overrides is None else copy.deepcopy(container_overrides)
        )
        self.node_overrides: NodeOverridesTypeDef = {} if node_overrides is None else node_overrides
        self.retry_strategy: RetryStrategyTypeDef = {} if retry_strategy is None else retry_strategy
        self.timeout: JobTimeoutTypeDef = {} if timeout is None else timeout

        # Add to container overrides
        if cpus is not None:
            self.container_overrides["vcpus"] = cpus
        if mem_mb is not None:
            self.container_overrides["memory"] = mem_mb
        if command is not None:
            self.container_overrides["command"] = copy.deepcopy(command)
        if instance_type is not None:
            self.container_overrides["instanceType"] = instance_type
        if environment is not None:
            self.container_overrides["environment"] = copy.deepcopy(environment)
        if resource_requirements is not None:
            self.container_overrides["resourceRequirements"] = copy.deepcopy(resource_requirements)

        self.job_id: Optional[str] = None

        self._waiters: Optional[Waiters] = None

    @classmethod
    def from_id(cls, client: batch.Client, job_id: str) -> "BatchJob":
        """"Builds a batch job from the given ID.

        Will lookup the job to retrieve job information.

        Args:
            client: the AWS batch client
            job_id: the job identifier
        """
        jobs_response = client.describe_jobs(jobs=[job_id])
        jobs = jobs_response["jobs"]
        assert len(jobs) == 1
        job_info = jobs[0]

        # Treat container overrides specially
        container: ContainerDetailTypeDef = job_info.get("container", {})
        container_overrides: ContainerOverridesTypeDef = {}
        if "vpus" in container:
            container_overrides["vcpus"] = container["vcpus"]
        if "memory" in container:
            container_overrides["memory"] = container["memory"]
        if "command" in container:
            container_overrides["command"] = container["command"]
        if "instanceType" in container:
            container_overrides["instanceType"] = container["instanceType"]
        if "environment" in container:
            container_overrides["environment"] = container["environment"]
        if "resourceRequirements" in container:
            container_overrides["resourceRequirements"] = container["resourceRequirements"]

        job: BatchJob = BatchJob(
            client=client,
            name=job_info["jobName"],
            queue=job_info["jobQueue"],
            job_definition=job_info["jobDefinition"],
            array_properties=job_info.get("arrayProperties", {}),
            depends_on=job_info.get("dependsOn", []),
            parameters=job_info.get("parameters", {}),
            container_overrides=container_overrides,
            retry_strategy=job_info.get("retryStrategy", {}),
            timeout=job_info.get("timeout", {}),
        )

        job.job_id = job_id

        return job

    @property
    def stream(self) -> Optional[str]:
        """The log stream for the job, if available."""
        return self.describe_job()["container"].get("logStreamName")

    def submit(self) -> SubmitJobResponseTypeDef:
        """Submits this job."""
        return self.client.submit_job(
            jobName=self.name,
            jobQueue=self.queue,
            arrayProperties=self.array_properties,
            dependsOn=self.depends_on,
            jobDefinition=self.job_definition_arn,
            parameters=self.parameters,
            containerOverrides=self.container_overrides,
            nodeOverrides=self.node_overrides,
            retryStrategy=self.retry_strategy,
            timeout=self.timeout,
        )

    def _reason(self, reason: Optional[str] = None) -> str:
        """The default reason for cancelling or terminating a job"""
        return reason if reason is not None else "manually initiated"

    def cancel_job(self, reason: Optional[str] = None) -> None:
        """Cancels the given AWS Batch job. Does nothing if running or finished."""
        assert self.job_id is not None, "Cannot cancel a job that has not been submitted"
        self.client.cancel_job(jobId=self.job_id, reason=self._reason(reason=reason))

    def terminate_job(self, reason: Optional[str] = None) -> None:
        """Terminates this job."""
        assert self.job_id is not None, "Cannot terminate a job that has not been submitted"
        self.client.terminate_job(jobId=self.job_id, reason=self._reason(reason=reason))

    def get_status(self) -> Optional[Status]:
        """Gets the status of this job"""
        if self.job_id is None:
            return None
        else:
            return Status(self.describe_job()["status"])

    def describe_job(self) -> JobDetailTypeDef:
        """Gets detauled information about this job."""
        jobs_response = self.client.describe_jobs(jobs=[self.job_id])
        job_statuses = jobs_response["jobs"]
        assert len(job_statuses) == 1
        job = job_statuses[0]
        assert (
            job["jobName"] == self.name
        ), f"""Job name mismatched: {self.name} != {job["jobName"]}"""
        assert (
            job["jobId"] == self.job_id
        ), f"""Job id mismatched: {self.job_id} != {job["jobId"]}"""
        assert (
            job["jobQueue"] == self.queue
        ), f"""Job queue mismatched: {self.queue} != {job["jobQueue"]}"""
        return job_statuses[0]

    def waiter(self, name: str, max_attempts: Optional[int] = None) -> botocore.waiter.Waiter:
        """Creates a waiter on which a caller can wait.

        Args:
            name: the name of the waiter to use
            max_attempts: the maximum # of attempts until reaching the given state.
        """
        if self._waiters is None:
            self._waiters = Waiters(client=self.client)
        waiter = self._waiters.get(name=name)
        waiter.config.max_attempts = sys.maxsize if max_attempts is None else max_attempts
        return waiter

    def _wait_on(
        self, states: List[str], max_attempts: Optional[int] = None, delay: Optional[int] = None
    ) -> batch.type_defs.JobDetailTypeDef:
        """Creates a waiter on which a caller can wait.

           Args:
               states: the list of states on which to wait
               max_attempts: the maximum # of attempts until reaching the given state.
               delay: the delay before waiting
        """
        for name in states:
            # Note: we could add some jitter to the delay (`waiter.config.delay`) in cases where
            # multiple concurrent batch jobs are polled.
            waiter = self.waiter(name=name)
            waiter.config.max_attempts = sys.maxsize if max_attempts is None else max_attempts
            if delay is not None:
                waiter.config.delay = delay
            waiter.wait(jobs=[self.job_id])

        return self.describe_job()

    def wait_on_exists(
        self, max_attempts: Optional[int] = None, delay: Optional[int] = None
    ) -> batch.type_defs.JobDetailTypeDef:
        """Creates a waiter that waits on the job to exist

            Args:
                max_attempts: the maximum # of attempts until reaching the given state.
                delay: the delay before waiting
         """
        return self._wait_on(states=["job-exists"], max_attempts=max_attempts, delay=delay)

    def wait_on_running(
        self, max_attempts: Optional[int] = None, delay: Optional[int] = None
    ) -> batch.type_defs.JobDetailTypeDef:
        """Creates a waiter that waits on the job to be running

            Args:
                max_attempts: the maximum # of attempts until reaching the given state.
                delay: the delay before waiting
         """
        return self._wait_on(
            states=["job-exists", "job-running"], max_attempts=max_attempts, delay=delay
        )

    def wait_on_complete(
        self, max_attempts: Optional[int] = None, delay: Optional[int] = None
    ) -> batch.type_defs.JobDetailTypeDef:
        """Creates a waiter that waits on the job to be completed (failed or succeeded)

            Args:
                max_attempts: the maximum # of attempts until reaching the given state.
                delay: the delay before waiting
         """
        return self._wait_on(
            states=["job-exists", "job-running", "job-complete"],
            max_attempts=max_attempts,
            delay=delay,
        )
