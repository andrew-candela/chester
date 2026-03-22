from datetime import datetime, timedelta
from edgar import Company, get_by_accession_number, set_identity
from edgar.entity.filings import EntityFilings, Filing

LOOKBACK_WINDOW_DAYS = 10
EDGAR_IDENTITY = "puntt.ai take_home_test@gmail.com"


class NullFilingError(Exception): ...  # noqa: E701


class EDGARDownloader:
    def __init__(self):
        set_identity(EDGAR_IDENTITY)

    @staticmethod
    def get_date_filter(start: str | None = None, end: str | None = "") -> str | None:
        """
        Returns the date range "slice" expected by the edgartools api.
        The format expected is seen in the extract_dates docs:
            extract_dates("2022-03-04") -> 2022-03-04, None, False
            extract_dates("2022-03-04:2022-04-05") -> 2022-03-04, 2022-04-05, True
            extract_dates("2022-03-04:") -> 2022-03-04, <current_date>, True
            extract_dates(":2022-03-04") -> 1994-07-01, 2022-03-04, True
        """
        if start is None and end is None:
            return None
        if start is None:
            start = (datetime.now() - timedelta(days=LOOKBACK_WINDOW_DAYS)).strftime(
                "%Y-%m-%d"
            )
        end = end if end is not None else ""
        return f"{start}:{end}"

    def get_filings(
        self, company_cik: str | int, date_filter: str | None = None
    ) -> EntityFilings:
        company = Company(company_cik)
        args = {}
        if date_filter is not None:
            args["filing_date"] = date_filter
        else:
            args["trigger_full_load"] = True
        return company.get_filings(**args)

    def get_by_accession_number(self, asc_number: str) -> Filing:
        filing = get_by_accession_number(asc_number)
        if filing is None:
            raise NullFilingError("Null filing")
        return filing
