"""
Temporal workflow and activities for SEC EDGAR file ingestion.

Here we translate the raw ingest logic into temporal actions/workflows
"""

import asyncio
from datetime import timedelta
from dataclasses import dataclass

from temporalio import activity, workflow
from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleSpec,
)
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError, ApplicationError


# TODO: I need to understand this better
with workflow.unsafe.imports_passed_through():
    from chester.lib.edgartools import EDGARDownloader, NullFilingError
    from chester.lib.serde import PickleStore, TextStore
    from chester import constants
    from edgar.entity.filings import Filing, EntityFilings


@dataclass
class RecentAccentionNumberParameters:
    company_cik: str
    start_date: str | None = None
    end_date: str | None = None


@activity.defn
async def fetch_filing_by_acc_number(accession_number: str) -> str:
    downloader = EDGARDownloader()
    try:
        filing = downloader.get_by_accession_number(accession_number)
    except NullFilingError:
        raise ApplicationError(
            message=f"No corresponding document for accession number: {accession_number}",
            non_retryable=True,
        )
    store = PickleStore()
    path_to_object = store.save(f"{accession_number}_raw", filing)
    return path_to_object


@activity.defn
async def scrub_filing(path_to_filing: str) -> str:
    store = PickleStore()
    filing: Filing = store.load(path_to_filing)
    scrubbed_filing = filing.to_dict()
    path = store.save(f"{filing.accession_no}_processed", scrubbed_filing)
    return path


@activity.defn
async def persist_filing(path_to_filing: str) -> None:
    pickle_store = PickleStore()
    filing: Filing = pickle_store.load(path_to_filing)
    text_store = TextStore()
    text_store.save(f"{filing.accession_no}_final.md", filing.to_context(detail="full"))


@activity.defn
async def fetch_recent_accession_numbers(
    params: RecentAccentionNumberParameters,
) -> list[str]:
    """
    Finds the accession numbers of all filings that occurred within the date range.
    If no date range is provided, returns all filings.
    """
    downloader = EDGARDownloader()
    date_range = EDGARDownloader.get_date_filter(params.start_date, params.end_date)
    filings: EntityFilings = downloader.get_filings(params.company_cik, date_range)
    print(f"{len(filings)=}")
    return sorted([f.accession_no for f in filings])


@workflow.defn
class IngestMultipleFilingWorkflow:
    @workflow.run
    async def run(self, params: RecentAccentionNumberParameters) -> None:
        accession_numbers = await workflow.execute_activity(
            fetch_recent_accession_numbers,
            params,
            task_queue=constants.INGESTION_TASK_QUEUE,
            start_to_close_timeout=timedelta(hours=1),
        )
        handles = []
        for accession_number in accession_numbers:
            try:
                handle = await workflow.start_child_workflow(
                    IngestSingleFilingWorkflow.run,
                    accession_number,
                    id=f"ingest-{accession_number}",
                    task_queue=constants.PROCESSING_TASK_QUEUE,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                )
                handles.append(handle)
            except WorkflowAlreadyStartedError:
                pass  # workflow is already done
        await asyncio.gather(*handles)


@workflow.defn
class IngestSingleFilingWorkflow:
    @workflow.run
    async def run(self, accession_number: str) -> None:
        path_to_raw = await workflow.execute_activity(
            fetch_filing_by_acc_number,
            accession_number,
            task_queue=constants.INGESTION_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
        )
        _ = await workflow.execute_activity(
            scrub_filing,
            path_to_raw,
            task_queue=constants.PROCESSING_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
        )
        _ = await workflow.execute_activity(
            persist_filing,
            path_to_raw,
            task_queue=constants.PROCESSING_TASK_QUEUE,
            start_to_close_timeout=timedelta(seconds=60),
        )


async def run_single_ingest_workflow(accession_number: str) -> None:
    client = await Client.connect(constants.TEMPORAL_SERVER_HOST)
    await client.execute_workflow(
        IngestSingleFilingWorkflow.run,
        accession_number,
        id=f"ingest-{accession_number}",
        task_queue=constants.PROCESSING_TASK_QUEUE,
        # Be careful! This won't make backfilling easy
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
    )


async def run_ingest_multiple_workflow(params: RecentAccentionNumberParameters) -> None:
    """
    This runs the initial ingestion and can run a backfill for some time period.
    """
    client = await Client.connect(constants.TEMPORAL_SERVER_HOST)
    if params.start_date is not None and params.end_date is not None:
        id = f"multi-ingest-{params.company_cik}-{params.start_date}-{params.end_date}"
    else:
        id = f"multi-ingest-{params.company_cik}"
    await client.execute_workflow(
        IngestMultipleFilingWorkflow.run,
        params,
        id=id,
        task_queue=constants.PROCESSING_TASK_QUEUE,
        # I want to allow duplicates here.
        # Child workflows are deduplicated based on acc number
        id_conflict_policy=WorkflowIDConflictPolicy.UNSPECIFIED,
        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    )


async def run_ongoing_ingestion_workflow(
    params: RecentAccentionNumberParameters,
) -> None:
    client = await Client.connect(constants.TEMPORAL_SERVER_HOST)

    # 2. Define the Schedule
    await client.create_schedule(
        f"ongoing_ingestion-{params.company_cik}",
        Schedule(
            action=ScheduleActionStartWorkflow(
                IngestMultipleFilingWorkflow.run,
                params,
                id=f"ongoing_ingestion-{params.company_cik}",
                task_queue=constants.PROCESSING_TASK_QUEUE,
            ),
            spec=ScheduleSpec(
                # Run every 5 minutes
                intervals=[ScheduleIntervalSpec(every=timedelta(minutes=10))],
            ),
        ),
    )
