"""Tests for :module:`~pyfgaws.batch.api`"""

from typing import List
from typing import Optional

import botocore.session
import pytest
from botocore.stub import Stubber
from mypy_boto3.batch import Client
from mypy_boto3.batch.type_defs import DescribeJobDefinitionsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import DescribeJobsResponseTypeDef  # noqa
from mypy_boto3.batch.type_defs import SubmitJobResponseTypeDef  # noqa
from py._path.local import LocalPath as TmpDir

from pyfgaws.batch import BatchJob
from pyfgaws.batch import Status
from pyfgaws.tests import stubbed_client


def stubbed_client_submit_job(
    submit_job_response: SubmitJobResponseTypeDef,
    describe_job_definitions_response: Optional[DescribeJobDefinitionsResponseTypeDef] = None,
) -> Client:
    client = botocore.session.get_session().create_client("batch", region_name="us-east-1")
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

        job = BatchJob(
            client=client, name="job-name", queue="job-queue", job_definition=job_definition
        )

        response = job.submit()

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
        # [Status.Failed],
        # [
        #     Status.Submitted,
        #     Status.Pending,
        #     Status.Runnable,
        #     Status.Runnable,
        #     Status.Running,
        #     Status.Succeeded,
        # ],
        # [
        #     Status.Submitted,
        #     Status.Pending,
        #     Status.Runnable,
        #     Status.Runnable,
        #     Status.Running,
        #     Status.Failed,
        # ],
    ],
)
def test_wait_for_job(statuses: List[Status]) -> None:
    service_responses: List[DescribeJobsResponseTypeDef] = []
    # add a response for the `describe_jobs` in `BatchJob.from_id`
    service_responses.append(build_describe_jobs_response(status=Status.Submitted))

    # add the expected responses
    service_responses.extend(build_describe_jobs_responses(*statuses))

    # add a three more terminal responses, since we have a waiter that's job-exists and then
    # job-running, before job-complete, with a final `describe_jobs` after it completes
    last_response = service_responses[-1]
    service_responses.append(last_response)
    service_responses.append(last_response)
    service_responses.append(last_response)

    assert len(service_responses) > 1
    client: Client = stubbed_client_describe_jobs(service_responses=service_responses)

    job: BatchJob = BatchJob.from_id(client=client, job_id="job-id")
    assert job.job_id is not None, str(job)
    response = job.wait_on_complete(delay=0)

    assert response == service_responses[-1]["jobs"][0], str(response)
