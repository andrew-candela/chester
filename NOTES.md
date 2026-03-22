
# Puntt.ai challenge notes

Documenting my plan and thought process.
This is mostly for my own mental clarity - I'm including it in case it helps the reviewer.

## Functional requirements

### Ingest relevant historical filings when a company is added to the system

The "system" in this case will be a DB table or a config file.
I'll start with DB table.
If I can get an alembic pipeline running then the DB route should be fine.

Ingestion means what's in the processing section below.

### Ingest new filings as they appear

This one is somewhat vague.
I can probably schedule a job to run every hour or so that checks for new filings.
I will probably have to maintain some kind of state like a high watermark or something.


### Backfill a date range for filings for a company


### Processing

For each filing:

- fetch metadata
- retrieve filing doc
- extract relevant info from filing doc
- persist result

## Soft Requirements

### No duplicates

Idempotent loads to the state DB.
Probably use the temporal workflowID as some compound key.
Find a way to uniquely identify each document.

### No data corruption.

Make sure the API requests fail fast and loud

### No extra processing

Will need some kind of state management.
Can use postgres for this.
Or maybe there is some temporal native feature I can use.

### Handle failures

- Retries for transient issues like timeouts or even 500's from the API
- if a workflow fails then temporal will show it
- document verification is out of scope for now, but if it were, 
we would want to persist documents that fail validation in the DB

## Understand the domain

I have to ingest the SEC 


## Temporal

I'm fairly new to temporal.
I'll have to plan out how to structure the application.

I want to have a long-running workflow poll the RSS feed

- state will grow. Think about continue as new. Can keep a local set of seen documents
Temporal stores the state in the external DB.
I can probably chunk seen IDs somehow.
- trigger child workflow to ingest any new filings

How do I trigger the child workflow?


