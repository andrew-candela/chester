"""
Launch the worker(s) and trigger the long-running controller workflows
"""

import asyncio
from temporalio.worker import Worker
from temporalio.client import Client
from chester import constants

from chester.ingestion import ingestion_actions


async def run_local_worker():
    """
    This worker can run as fast as I can schedule it
    """
    client = await Client.connect(constants.TEMPORAL_SERVER_HOST)
    worker = Worker(
        client,
        task_queue=constants.PROCESSING_TASK_QUEUE,
        workflows=[
            ingestion_actions.IngestSingleFilingWorkflow,
            ingestion_actions.IngestMultipleFilingWorkflow,
        ],
        activities=[
            ingestion_actions.scrub_filing,
            ingestion_actions.persist_filing,
        ],
    )
    await worker.run()


async def run_edgar_api_worker():
    """
    This worker is subject to the edgar API rate limits
    """
    client = await Client.connect(constants.TEMPORAL_SERVER_HOST)
    worker = Worker(
        client,
        task_queue=constants.INGESTION_TASK_QUEUE,
        workflows=[
            ingestion_actions.IngestSingleFilingWorkflow,
            ingestion_actions.IngestMultipleFilingWorkflow,
        ],
        activities=[
            ingestion_actions.fetch_filing_by_acc_number,
            ingestion_actions.fetch_recent_accession_numbers,
        ],
        max_activities_per_second=constants.MAX_EDGAR_TASKS_PER_SECOND,
    )
    await worker.run()


async def main():
    await asyncio.gather(run_local_worker(), run_edgar_api_worker())


def run():
    print("Launching a Chester worker. Please allow this process to continue to run...")
    asyncio.run(main())


if __name__ == "__main__":
    run()
