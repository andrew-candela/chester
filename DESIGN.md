
# Ingestion engine design

## Decisions and tradeoffs

### Architecture

I settled on Temporal for this project fairly quickly.
I also considered Airflow and Celery.

#### Airflow (no)

Airflow seemed like an OK fit for this at first, since such an emphasis was
placed on replayability and auditability.
The airflow UI is great for observing any failed tasks and retriggering/retrying.
It has great tools for managing throughput as well, which is a big concern with 3rd party APIs.

Airflow wasn't a great fit for this for a few reasons.

Airflow is not great at scheduling a variable number of tasks.
There are some tools now that let you do this, but Airflow DAGs are still best when they are relatively static.
Waiting around for new filings to show up and then triggering an ingestion is probably workable, but would be a pain.

Airflow is also not great at scheduling many small tasks quickly.
Each task runs a completely separate process which must parse the whole DAG and run an airflow runtime.
This time would add up if we are ingesting thousands of files.

Airflow also wouldn't be great at deduplicating workflows.
If I wanted to ingest filings for a particular date range that I may have partially completed,
it would be a pain to prevent re-download of filings that I've already got.

#### Celery

Celery would work for this, and it would probably work well.
The problem is that it would take me forever to fulfill the requirements.
I'd have to track the downloaded documents in some application state somewhere and refer to it from every task.
I'd also struggle with the UI. I'm not aware of any out of the box celery UI that would offer much
visibility into the workflow progress.
I'd have to integrate with something like sentry for a reasonable observability experience.

#### Temporal

Temporal checks all the boxes for this assignment.
It has a reasonable UI where you can easily check failed workflows and inspect input/output and stacktraces.
It persists intermediate state, and has some built-in task deduplication tools available.
It's also fairly fast. If you're not passing a ton of data between the back-end DB and the workers, then scheduling is fast.
It also has a robust set of tools to manage concurrency.

Also, I want to learn more about temporal, and I know it's what the team at Puntt is using.

#### Application design

I had to cut some corners here, so I built with the following in mind:

- keep as much state as possible out of the temporal metadata DB.
I serialized/deserialized intermediate data to/from local disk.
- use temporal workflow IDs to prevent duplication of work. All documents are identified by a unique accession_number.
Maybe there is a more elegant way of doing this than the one I found, but I'm new to temporal...

New companies are added to a constants file in the application code, and workflows exist to ingest all available docs
or ingest all docs filed within a certain date range.
Workflow can also be scheduled to run intermittently to check for filings made in the last N days.
Workflows are scheduled to ingest any files returned by the last N day activity. Temporal prevents running workflows for files that have been ingested already. It's kludgy, but it was easy to implement.

"Correctness" of the downloaded documents is done in kind of a kludgey way.
The edgartools library offers an interface that created a `Filing` object.
When I "persist" this object, I'm writing a pickled version of it to disk.
My "validation" step loads this object from disk (ensuring it is not currupted)
and performs some operation on it.
While it is possible that the original document itself is incomplete, I find it unlikely that
my pipeline introduces any invisible deterioration of the data.

### Tradeoffs

I HEAVILY optimized for time savings in this project.
Ideally, a project like this would have reasonable logging (mine has basically none),
persist intermediate data to S3 (or similar)
and persist the final results to a database (or similar).
My application uses the local filesystem for persistence, purely for ease of implementation.
The application logic is correct, but it will obviously not work in a realistic environment.

I also skimped heavily on actual utility of the application.
For example, my "validation" is basically a no-op, and "persistence" is writing some files to disk.
This I believe is fine because it seems that the spirit of the exercise is not about
actually extracting value from these documents, but rather demonstrating a pipeline with the desired capabilities.
I hope I'm correct.

## Production Readiness

This would be reasonable to deploy in AWS.
I would largely copy the structure of the compose file in compose/docker-compose-*.yml.
I got these files from the official Temporal example repo.
We could run the workers and temporal components (server, UI etc) as ECS tasks (or similar),
and then other applications could submit workflows.
Postgres (or similar) could be RDS, and we'd store intermediate results from activities/workflows to S3.

### Monitoring and Alerting

Temporal seems like it suffers a similar footgun as airflow - i.e. you should be careful about what
you send to the metadata DB.
It's probably very easy to fill up or otherwise degrade performance of the database by
passing huge blobs from one workflow activity to the next.
I'd want to keep a close eye on DB metrics such as:

- disk space
- longest running queries
- CPU & memory of course.

I'd also be very keen on keeping an eye on the number of active workflows.
If we've got some systemic problem and workflows are retrying forever in some bad state,
I'd expect running workflows to grow linearly as more are added and none are cleared.

To diagnose a stuck or failed filing, you could check to see if some workflows have been running for more than N minutes,
or else check for activities that are on their i'th attempt.
There are some conditions in my implementation where the workflow simply fails,
(some accession numbers are not retrievable), and in this case you can just look at the Temporal UI for failed workflows.

Handling the shared rate limit of the EDGAR API is easy.
Create a worker/workers that handle JUST the EDGAR api operations.
You can set the global scheduling rate for these workers to max of 10/s.
It's not quite a perfect application wide api rate limit - we almost certainly leave some performance on the table,
but it stops us from getting throttled.

## Future improvements

### What would you build with another day of work

Obviously I'd like to do a realistic processing of these documents.
The edgartools library has a cool feature where they produce instructions intended to be consumed by an LLM
that suggest how you'd interact with each doc.
I'm currently persisting this along with the pickled raw document state, so theoretically an agent could
programmatically interact with these docs.

Also logging and alerting would be good.
I probably also have to think about timeouts/etc.

Also my implementation of incremental loads is kludgy.
There is probably a better way to get new files from monitoring an RSS feed.

### How would you track 500+ companies

- I'd need more workers for sure,
- I'd probably have to scale my metadata DB,
- I'd have to use S3 (or similar) for intermediate data.

Otherwise I think it would do OK as it is to scale to more companies.

### How would you handle new processing stages/handle changes to the EDGAR API

This is a good question.
I'm fairly new to temporal, so I don't have any hands-on experience doing this.
From what I gather, versioning workflows is very important.
If I try to modify an existing workflow with new activities etc, I will have to be careful to make the
changes backwards compatible via versioning.

If I wanted to add processing steps to workflows that have already completed, this would be much harder.
Worst case scenario, I could create a new "updater" workflow that grabs intermediate data from S3,
does the new/different steps as needed, and then persists the new/updated data.

If the EDGAR API changes and I don't need to re-run existing workflows, that's easy.
I can update the application code, push it to the workers,
and then the next iteration of the workflows will have the updated code.
