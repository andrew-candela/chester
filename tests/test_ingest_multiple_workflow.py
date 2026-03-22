"""
Tests for IngestMultipleFilingWorkflow / run_ingest_multiple_workflow.

Verified behaviors:
  1. The workflow executes the full activity pipeline (fetch → scrub → persist)
     exactly once for every accession number returned by
     fetch_recent_accession_numbers.
  2. Re-running the workflow a second time does NOT re-process filings whose
     child workflow (IngestSingleFilingWorkflow) already completed successfully.
     The idempotency guarantee comes from
     WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY used when starting child
     workflows: a successful prior run produces a WorkflowAlreadyStartedError
     which the parent silently catches and skips.
"""

from contextlib import asynccontextmanager

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from chester import constants
from chester.ingestion.ingestion_actions import (
    IngestMultipleFilingWorkflow,
    IngestSingleFilingWorkflow,
    RecentAccentionNumberParameters,
)

# ---------------------------------------------------------------------------
# Fake data
# ---------------------------------------------------------------------------

FAKE_ACCESSION_NUMBERS = [
    "0000320193-99-000001",
    "0000320193-99-000002",
    "0000320193-99-000003",
]

# Parameters that will never match real SEC filings (year 2099).
FAKE_PARAMS = RecentAccentionNumberParameters(
    company_cik="0000320193",
    start_date="2099-01-01",
    end_date="2099-02-01",
)

# ---------------------------------------------------------------------------
# Mock activity factory
# ---------------------------------------------------------------------------


def make_mock_activities(call_counts: dict):
    """Return mock activity callables that increment *call_counts* on each call.

    The activities are registered under the same names used by the real
    workflows so that workers resolve them correctly.
    """

    @activity.defn(name="fetch_recent_accession_numbers")
    async def mock_fetch_recent_accession_numbers(
        params: RecentAccentionNumberParameters,
    ) -> list[str]:
        return FAKE_ACCESSION_NUMBERS

    @activity.defn(name="fetch_filing_by_acc_number")
    async def mock_fetch_filing_by_acc_number(accession_number: str) -> str:
        call_counts["fetch"] += 1
        return f"{accession_number}_raw"

    @activity.defn(name="scrub_filing")
    async def mock_scrub_filing(path_to_filing: str) -> str:
        call_counts["scrub"] += 1
        return f"{path_to_filing}_processed"

    @activity.defn(name="persist_filing")
    async def mock_persist_filing(path_to_filing: str) -> None:
        call_counts["persist"] += 1

    return (
        mock_fetch_recent_accession_numbers,
        mock_fetch_filing_by_acc_number,
        mock_scrub_filing,
        mock_persist_filing,
    )


# ---------------------------------------------------------------------------
# Worker context manager
# ---------------------------------------------------------------------------


@asynccontextmanager
async def running_workers(env: WorkflowEnvironment, activities_tuple):
    """Start the ingestion and processing workers needed to execute the workflows."""
    mock_fetch_recent, mock_fetch_filing, mock_scrub, mock_persist = activities_tuple

    # Ingestion queue: handles the two network-bound fetch activities.
    async with Worker(
        env.client,
        task_queue=constants.INGESTION_TASK_QUEUE,
        activities=[mock_fetch_recent, mock_fetch_filing],
    ):
        # Processing queue: runs both workflow types and the transformation activities.
        async with Worker(
            env.client,
            task_queue=constants.PROCESSING_TASK_QUEUE,
            workflows=[IngestMultipleFilingWorkflow, IngestSingleFilingWorkflow],
            activities=[mock_scrub, mock_persist],
        ):
            yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_workflow_processes_all_returned_accession_numbers():
    """The full activity pipeline runs exactly once per accession number."""
    call_counts = {"fetch": 0, "scrub": 0, "persist": 0}

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with running_workers(env, make_mock_activities(call_counts)):
            await env.client.execute_workflow(
                IngestMultipleFilingWorkflow.run,
                FAKE_PARAMS,
                id="test-process-all",
                task_queue=constants.PROCESSING_TASK_QUEUE,
            )

    n = len(FAKE_ACCESSION_NUMBERS)
    assert call_counts["fetch"] == n, (
        "fetch_filing_by_acc_number should be called "
        f"{n} times, got {call_counts['fetch']}"
    )
    assert call_counts["scrub"] == n, (
        f"scrub_filing should be called {n} times, got {call_counts['scrub']}"
    )
    assert call_counts["persist"] == n, (
        f"persist_filing should be called {n} times, got {call_counts['persist']}"
    )


@pytest.mark.asyncio
async def test_rerun_skips_previously_successful_filings():
    """A second run of IngestMultipleFilingWorkflow must not reprocess filings
    whose IngestSingleFilingWorkflow child already completed successfully.

    Mechanism: child workflows are started with
    WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY.  A prior successful
    completion causes start_child_workflow to raise WorkflowAlreadyStartedError,
    which the parent catches and treats as "already done".  As a result, the
    per-filing activities are never scheduled again.
    """
    call_counts = {"fetch": 0, "scrub": 0, "persist": 0}

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with running_workers(env, make_mock_activities(call_counts)):
            # First run: all accession numbers are new, every activity fires.
            await env.client.execute_workflow(
                IngestMultipleFilingWorkflow.run,
                FAKE_PARAMS,
                id="test-idempotent-run-1",
                task_queue=constants.PROCESSING_TASK_QUEUE,
            )

            counts_after_first_run = dict(call_counts)

            # Second run: same accession numbers — child workflows already
            # succeeded, so they must be skipped entirely.
            await env.client.execute_workflow(
                IngestMultipleFilingWorkflow.run,
                FAKE_PARAMS,
                id="test-idempotent-run-2",
                task_queue=constants.PROCESSING_TASK_QUEUE,
            )

    assert counts_after_first_run["fetch"] == len(FAKE_ACCESSION_NUMBERS), (
        "First run should have processed every accession number"
    )
    assert call_counts["fetch"] == counts_after_first_run["fetch"], (
        "fetch_filing_by_acc_number must not be called again on second run"
    )
    assert call_counts["scrub"] == counts_after_first_run["scrub"], (
        "scrub_filing must not be called again on second run"
    )
    assert call_counts["persist"] == counts_after_first_run["persist"], (
        "persist_filing must not be called again on second run"
    )
