"""Tests for :module:`~pyfgaws.batch.api`"""

import enum
from pathlib import Path
from typing import List
from typing import Literal
from typing import Optional

import botocore.session
import pytest
from botocore.stub import Stubber
from mypy_boto3.batch import Client
from mypy_boto3.batch.type_defs import DescribeJobDefinitionsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa
from py._path.local import LocalPath as TmpDir

from pyfgaws.batch import submit_job
from pyfgaws.batch import wait_for_job
from pyfgaws.tests import stubbed_client


@enum.unique
class Status(enum.Enum):
    Submitted = "SUBMITTED"
    Pending = "PENDING"
    Runnable = "RUNNABLE"
    Starting = "STARTING"
    Running = "RUNNING"
    Succeeded = "SUCCEEDED"
    Failed = "FAILED"


StatusType = Literal[
    "SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED"
]


def stubbed_client_submit_job(
    submit_job_response: SubmitJobResponseTypeDef,
    describe_job_definitions_response: Optional[DescribeJobDefinitionsResponseTypeDef] = None,
) -> Client:
    client = botocore.session.get_session().create_client("batch")
    stubber = Stubber(client)

    if describe_job_definitions_response is not None:
        stubber.add_response(
            method="describe_job_definitions", service_response=describe_job_definitions_response
        )

    stubber.add_response(method="submit_job", service_response=submit_job_response)

    stubber.activate()
    return client


def stubbed_client_describe_jobs(service_responses: List[DescribeJobsResponseTypeDef]) -> Client:
    return stubbed_client(
        service_name="batch", method="describe_jobs", service_responses=service_responses
    )


def valid_describe_job_definitions_response() -> DescribeJobDefinitionsResponseTypeDef:
    return {
        "jobDefinitions": [
            {
                "type": "container",
                "jobDefinitionName": "job-definition-name",
                "jobDefinitionArn": "arn:aws:batch:some-arn-1",
                "revision": 1,
            },
            {
                "type": "container",
                "jobDefinitionName": "job-definition-name",
                "jobDefinitionArn": "arn:aws:batch:some-arn-2",
                "revision": 2,
            },
            {
                "type": "container",
                "jobDefinitionName": "job-definition-name",
                "jobDefinitionArn": "arn:aws:batch:some-arn-3",
                "revision": 3,
            },
        ],
        "nextToken": "next-token",
    }


def valid_submit_job_response() -> SubmitJobResponseTypeDef:
    return {"jobName": "job-name", "jobId": "job-id"}


def test_submit_job(tmpdir: TmpDir) -> None:

    template_json: Path = Path(tmpdir) / "template.json"
    with template_json.open("w") as writer:
        writer.write(
            """
            {
                "batchJob": {
                    "containerOverrides": {
                    },
                    "jobName": "job-name",
                    "jobQueue": "job-queue"
                }
            }
            """
        )

    for job_definition in ["job-definition-name", "arn:aws:batch:some-arn"]:
        client: Client
        submit_job_response: SubmitJobResponseTypeDef = valid_submit_job_response()
        describe_job_definitions_response: Optional[DescribeJobDefinitionsResponseTypeDef] = None

        if not job_definition.startswith("arn:aws:batch:"):
            describe_job_definitions_response = valid_describe_job_definitions_response()

        client = stubbed_client_submit_job(
            submit_job_response=submit_job_response,
            describe_job_definitions_response=describe_job_definitions_response,
        )

        response = submit_job(
            batch_client=client, template_json=template_json, job_definition=job_definition
        )

        assert response == submit_job_response


def build_describe_jobs_response(status: Status) -> DescribeJobsResponseTypeDef:
    return {
        "jobs": [
            {
                "jobName": "job-name",
                "jobId": "job-id",
                "jobQueue": "job-queue",
                "jobDefinition": "arn:aws:batch:some-arn",
                "startedAt": 1,
                "status": status.value,
            }
        ]
    }


def build_describe_jobs_responses(*status: Status) -> List[DescribeJobsResponseTypeDef]:
    assert all(s in Status for s in status)
    return [build_describe_jobs_response(status=s) for s in status]


@pytest.mark.parametrize(
    "statuses",
    [
        [Status.Succeeded],
        [Status.Failed],
        [
            Status.Submitted,
            Status.Pending,
            Status.Runnable,
            Status.Runnable,
            Status.Running,
            Status.Succeeded,
        ],
        [
            Status.Submitted,
            Status.Pending,
            Status.Runnable,
            Status.Runnable,
            Status.Running,
            Status.Failed,
        ],
    ],
)
def test_wait_for_job(statuses: List[Status]) -> None:
    service_responses = build_describe_jobs_responses(*statuses)
    assert len(service_responses) > 0
    client: Client = stubbed_client_describe_jobs(service_responses=service_responses)

    response = wait_for_job(batch_client=client, job_id="job-id", polling_interval=0)

    assert response == service_responses[-1], str(response)
