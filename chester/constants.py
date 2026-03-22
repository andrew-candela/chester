import os


EMAIL = os.getenv("CHESTER__EMAIL", "name@domain.com")
COMPANY_NAME = "PUNTT.IO Test"
COMPANIES = {
    "Apple": "0000320193",
    "Microsoft": "0000789019",
    "Tesla": "0001318605",
}
INGESTION_TASK_QUEUE = "edgar-ingestion"
PROCESSING_TASK_QUEUE = "edgar-processing"
TEMPORAL_SERVER_HOST = "localhost:7233"
MAX_EDGAR_TASKS_PER_SECOND = 10
# Super hack. Just sharing the temporal DB
CHESTER_DATABASE_URL = "postgresql://temporal:temporal@localhost:5432/temporal"
CHESTER_CACHE_DIR = "tmp/chester"
