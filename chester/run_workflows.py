"""
Super hack to allow users to run workflows.
Counts the number of args passed to the script and decides to run a full sync & schedule
or a backfill with provided dates.
"""

import asyncio
import sys
from dataclasses import dataclass

from chester.ingestion.ingestion_actions import (
    # run_single_ingest_workflow,
    run_ingest_multiple_workflow,
    run_ongoing_ingestion_workflow,
    RecentAccentionNumberParameters,
)
from chester import constants


@dataclass
class WorkflowRunArgs:
    schedule: bool
    start_date: str | None
    end_date: str | None


def parse_args() -> WorkflowRunArgs:
    # No args provided. Run initial sync and schedule ongoing syncs
    if len(sys.argv) == 1:
        return WorkflowRunArgs(True, None, None)
    if len(sys.argv) == 3:
        return WorkflowRunArgs(False, sys.argv[1], sys.argv[2])
    raise ValueError("Wrong number of arguments detected")


async def run_initial_workflow() -> None:
    coroutines = []
    for cik in constants.COMPANIES.values():
        historical_task = run_ingest_multiple_workflow(
            RecentAccentionNumberParameters(
                company_cik=cik,
            )
        )
        ongoing_task = run_ongoing_ingestion_workflow(
            RecentAccentionNumberParameters(
                company_cik=cik,
                end_date="",
            )
        )
        coroutines.extend([historical_task, ongoing_task])

    await asyncio.gather(*coroutines)


async def run_backfill_workflow(args: WorkflowRunArgs) -> None:
    coroutines = []
    for cik in constants.COMPANIES.values():
        backfill_task = run_ingest_multiple_workflow(
            RecentAccentionNumberParameters(
                company_cik=cik,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        )
        coroutines.extend([backfill_task])

    await asyncio.gather(*coroutines)


async def main():
    args = parse_args()

    if args.schedule:
        return await run_initial_workflow()
    return await run_backfill_workflow(args)


def run():
    asyncio.run(main())


if __name__ == "__main__":
    run()
