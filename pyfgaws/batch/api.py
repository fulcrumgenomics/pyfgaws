"""
Utility methods for interacting with AWS Batch
----------------------------------------------
"""
import copy
import enum
import logging
import signal
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Union

import botocore
import mypy_boto3_batch as batch
import namegenerator
from botocore.waiter import Waiter as BotoWaiter
from mypy_boto3_batch.type_defs import ArrayPropertiesTypeDef  # noqa
from mypy_boto3_batch.type_defs import ContainerDetailTypeDef  # noqa
from mypy_boto3_batch.type_defs import ContainerOverridesTypeDef  # noqa
from mypy_boto3_batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3_batch.type_defs import JobDependencyTypeDef  # noqa
from mypy_boto3_batch.type_defs import JobDetailTypeDef  # noqa
from mypy_boto3_batch.type_defs import JobTimeoutTypeDef  # noqa
from mypy_boto3_batch.type_defs import JobTimeoutTypeDef  # noqa
from mypy_boto3_batch.type_defs import KeyValuePairTypeDef  # noqa
from mypy_boto3_batch.type_defs import NodeOverridesTypeDef  # noqa
from mypy_boto3_batch.type_defs import ResourceRequirementTypeDef  # noqa
from mypy_boto3_batch.type_defs import RetryStrategyTypeDef  # noqa
from mypy_boto3_batch.type_defs import SubmitJobResponseTypeDef  # noqa

# The possible values of Status, for type checking
StatusValue = Union[
    Literal["SUBMITTED"],
    Literal["PENDING"],
    Literal["RUNNABLE"],
    Literal["STARTING"],
    Literal["RUNNING"],
    Literal["SUCCEEDED"],
    Literal["FAILED"],
]


@enum.unique
class Status(enum.Enum):
    """Enumeration for statuses of an AWS Batch Job.

    Attributes:
        status (str): The status string of the AWS Batch Job
        logs (bool): True if this status has CloudWatch logs, False otherwise.
    """

    Submitted = ("SUBMITTED", False)
    Pending = ("PENDING", False)
    Runnable = ("RUNNABLE", False)
    Starting = ("STARTING", False)
    Running = ("RUNNING", True)
    Succeeded = ("SUCCEEDED", True)
    Failed = ("FAILED", True)

    def __init__(self, status: StatusValue, logs: bool) -> None:
        self.status: StatusValue = status
        self.logs: bool = logs

    @staticmethod
    def from_string(status: StatusValue) -> "Status":
        """Builds a status from a string, case insensitive."""
        status_lower = status.lower()
        return next(s for s in Status if s.status.lower() == status_lower)


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


# Valid keys for ContainerOverridesTypeDef
_ContainerOverridesTypes = Union[
    Literal["vcpus"],
    Literal["memory"],
    Literal["command"],
    Literal["instanceType"],
    Literal["environment"],
    Literal["resourceRequirements"],
]


class BatchJob:
    """Stores information about a batch job.

    The following arguments override the provided container overrides: `cpus`, `mem_mb` (
    overrides `memory`), `command`, `instance_type`, `environment`, and `resourceRequirements`.

    Attributes:
        client: the batch client to use
        queue: the nae of the AWS batch queue
        job_definition: the ARN for the AWS batch job definition, or the name of the job definition
            to get the latest revision
        name: the name of the job, otherwise one will be automatically generated
        cpus: the number of CPUs to request
        mem_mb: the amount of memory to request (in megabytes)
        command: the command to use
        instance_type: the instance type to use
        environment: the environment variables to use
        resource_requirements: a list of resource requirements for the job (type and amount)
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
        cancel_on: cancel a submitted batch job if one of the given signals are encountered.
        terminate_on: terminate a submitted batch job if one of the given signals are
            encountered; this will take precedence over cancel.
    """

    def __init__(
        self,
        client: batch.Client,
        queue: str,
        job_definition: str,
        name: Optional[str] = None,
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
        cancel_on: Optional[List[int]] = None,
        terminate_on: Optional[List[int]] = None,
    ) -> None:

        self.client: batch.Client = client

        # Get the latest job definition ARN if not given
        self.job_definition_arn: str
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
        self.name: str = namegenerator.gen() if name is None else name
        self.queue: str = queue
        self.array_properties: Optional[ArrayPropertiesTypeDef] = array_properties
        self.depends_on: Optional[List[JobDependencyTypeDef]] = copy.deepcopy(depends_on)
        self.parameters: Optional[Dict[str, Any]] = copy.deepcopy(parameters)
        self.container_overrides: Optional[ContainerOverridesTypeDef] = (
            copy.deepcopy(container_overrides)
        )
        self.node_overrides: Optional[NodeOverridesTypeDef] = node_overrides
        self.retry_strategy: Optional[RetryStrategyTypeDef] = retry_strategy
        self.timeout: Optional[JobTimeoutTypeDef] = timeout

        # Add to container overrides
        self._add_to_container_overrides(key="vcpus", value=cpus)
        self._add_to_container_overrides(key="memory", value=mem_mb)
        self._add_to_container_overrides(key="command", value=copy.deepcopy(command))
        self._add_to_container_overrides(key="instanceType", value=instance_type)
        self._add_to_container_overrides(key="environment", value=copy.deepcopy(environment))
        self._add_to_container_overrides(
            key="resourceRequirements", value=copy.deepcopy(resource_requirements)
        )

        self.job_id: Optional[str] = None

        self.cancel_on: Optional[List[int]] = cancel_on
        self.terminate_on: Optional[List[int]] = terminate_on

    def _add_to_container_overrides(
        self, key: _ContainerOverridesTypes, value: Optional[Any]
    ) -> None:
        """Adds the given value to the container overrides for the given key"""
        if value is not None:
            if self.container_overrides is None:
                self.container_overrides = {}
            self.container_overrides[key] = value

    @classmethod
    def from_id(
        cls,
        client: batch.Client,
        job_id: str,
        cancel_on: Optional[List[int]] = None,
        terminate_on: Optional[List[int]] = None,
    ) -> "BatchJob":
        """"Builds a batch job from the given ID.

        Will lookup the job to retrieve job information.

        Args:
            client: the AWS batch client
            job_id: the job identifier
            cancel_on: cancel a submitted batch job if one of the given signals are encountered.
            terminate_on: terminate a submitted batch job if one of the given signals are
                encountered; this will take precedence over cancel.
        """
        jobs_response = client.describe_jobs(jobs=[job_id])
        jobs = jobs_response["jobs"]
        assert len(jobs) <= 1, "More than one job described"
        assert len(jobs) > 0, "No jobs found"
        job_info = jobs[0]

        # Treat container overrides specially
        container: ContainerDetailTypeDef = job_info.get("container", {})
        container_overrides: Optional[ContainerOverridesTypeDef] = None

        def add_to_container_overrides(key: _ContainerOverridesTypes) -> None:
            nonlocal container_overrides
            nonlocal container
            if key in container:
                if container_overrides is None:
                    container_overrides = {}
                container_overrides[key] = container[key]

        add_to_container_overrides(key="vcpus")
        add_to_container_overrides(key="memory")
        add_to_container_overrides(key="command")
        add_to_container_overrides(key="instanceType")
        add_to_container_overrides(key="environment")
        add_to_container_overrides(key="resourceRequirements")

        job: BatchJob = BatchJob(
            client=client,
            name=job_info["jobName"],
            queue=job_info["jobQueue"],
            job_definition=job_info["jobDefinition"],
            array_properties=job_info.get("arrayProperties"),
            depends_on=job_info.get("dependsOn"),
            parameters=job_info.get("parameters"),
            container_overrides=container_overrides,
            retry_strategy=job_info.get("retryStrategy"),
            timeout=job_info.get("timeout"),
            cancel_on=cancel_on,
            terminate_on=terminate_on,
        )

        job.job_id = job_id

        return job

    @property
    def stream(self) -> Optional[str]:
        """The log stream for the job, if available."""
        return self.describe_job()["container"].get("logStreamName")

    def submit(self) -> SubmitJobResponseTypeDef:
        """Submits this job."""

        # If we have arguments that are None, then we have **not** include them as keyword
        # arguments
        # See: https://github.com/vemel/mypy_boto3_builder/issues/30
        # See: https://github.com/boto/botocore/issues/2075
        kwargs: Dict[str, Any] = {}

        def add_to_kwargs(key: str, value: Optional[Any]) -> None:
            if value is not None:
                kwargs[key] = value

        add_to_kwargs(key="arrayProperties", value=self.array_properties)
        add_to_kwargs(key="dependsOn", value=self.depends_on)
        add_to_kwargs(key="containerOverrides", value=self.container_overrides)
        add_to_kwargs(key="parameters", value=self.parameters)
        add_to_kwargs(key="nodeOverrides", value=self.node_overrides)
        add_to_kwargs(key="retryStrategy", value=self.retry_strategy)
        add_to_kwargs(key="timeout", value=self.timeout)

        response = self.client.submit_job(
            jobName=self.name,
            jobQueue=self.queue,
            jobDefinition=self.job_definition_arn,
            **kwargs,
            # arrayProperties=self.array_properties,
            # dependsOn=self.depends_on,
            # containerOverrides=self.container_overrides,
            # parameters=self.parameters,
            # nodeOverrides=self.node_overrides,
            # retryStrategy=self.retry_strategy,
            # timeout=None,
        )

        # Don't forget to set the job id
        self.job_id = response["jobId"]

        return response

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
            return Status.from_string(self.describe_job()["status"])

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

    def wait_on(
        self,
        status_to_state: Dict[Status, bool],
        max_attempts: Optional[int] = None,
        delay: Optional[int] = None,
        after_success: bool = False,
        terminate_on_signal: bool = False,
    ) -> batch.type_defs.JobDetailTypeDef:
        """Waits for the given states with associated success or failure.

        If some states are missing from the input mapping, then all statuses after the last
        successful input status are treated as success or failure based on `after_success`

        Args:
            status_to_state: mapping of status to success (true) or failure (false) state
            max_attempts: the maximum # of attempts until reaching the given state.
            delay: the delay before waiting
            after_success: true to treat all status after the last successful input status are
                treated as success, otherwise failure.
        """
        assert len(status_to_state) > 0, "No statuses given"
        assert any(value for value in status_to_state.values()), "No statuses with success set."

        _status_to_state = copy.deepcopy(status_to_state)
        # get the last status in the given mapping
        last_success_status = None
        for status in Status:
            if _status_to_state.get(status, False):
                last_success_status = status

        # for all statuses after last_success_status, set to failure
        set_to_failure = False
        for status in Status:
            if status == last_success_status:
                set_to_failure = True
            elif set_to_failure:
                _status_to_state[status] = after_success

        name = "Waiter for statues: [" + ",".join(s.status for s in _status_to_state) + "]"
        config: Dict[str, Any] = {"version": 2}
        waiter_body: Dict[str, Any] = {
            "delay": 1 if delay is None else delay,
            "operation": "DescribeJobs",
            "maxAttempts": sys.maxsize if max_attempts is None else max_attempts,
            "acceptors": [
                {
                    "argument": "jobs[].status",
                    "expected": f"{status.status}",
                    "matcher": "pathAll",
                    "state": f"""{"success" if state else "failure"}""",
                }
                for status, state in _status_to_state.items()
            ],
        }
        config["waiters"] = {name: waiter_body}
        model: botocore.waiter.WaiterModel = botocore.waiter.WaiterModel(config)
        waiter: BotoWaiter = botocore.waiter.create_waiter_with_client(name, model, self.client)
        waiter.wait(jobs=[self.job_id])

        if self.cancel_on is not None:
            for code in self.cancel_on:
                signal.signal(
                    code, lambda signum, frame: self.cancel_job(reason=f"Interrupted: {code}")
                )

        if self.terminate_on is not None:
            for code in self.terminate_on:
                signal.signal(
                    code, lambda signum, frame: self.terminate_job(reason=f"Interrupted: {code}")
                )

        return self.describe_job()

    def wait_on_running(
        self, max_attempts: Optional[int] = None, delay: Optional[int] = None
    ) -> batch.type_defs.JobDetailTypeDef:
        """Waits for the given states with associated success or failure.

        Args:
            max_attempts: the maximum # of attempts until reaching the given state.
            delay: the delay before waiting
        """
        return self.wait_on(
            status_to_state={Status.Running: True},
            max_attempts=max_attempts,
            delay=delay,
            after_success=True,
        )

    def wait_on_complete(
        self, max_attempts: Optional[int] = None, delay: Optional[int] = None
    ) -> batch.type_defs.JobDetailTypeDef:
        """Waits for the given states with associated success or failure.

        Args:
            max_attempts: the maximum # of attempts until reaching the given state.
            delay: the delay before waiting
        """
        return self.wait_on(
            status_to_state={Status.Succeeded: True, Status.Failed: True},
            max_attempts=max_attempts,
            delay=delay,
            after_success=False,
        )
