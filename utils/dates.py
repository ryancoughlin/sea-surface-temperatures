from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

class DateFormatter:
    @staticmethod
    def get_query_date(date: datetime, lag_days: int = 0) -> datetime:
        """
        Get standardized query date accounting for timezone and lag days.
        Returns date in UTC.
        """
        # Ensure date is in UTC
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        
        # Adjust for lag days
        query_date = date - timedelta(days=lag_days)
        
        # Return start of day in UTC
        return datetime(
            query_date.year,
            query_date.month,
            query_date.day,
            tzinfo=timezone.utc
        )

    @staticmethod
    def format_cmems_date(date: datetime) -> Tuple[str, str]:
        """Format date range for CMEMS API."""
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
            
        start = date.strftime("%Y-%m-%dT00:00:00Z")
        end = date.strftime("%Y-%m-%dT23:59:59Z")
        return start, end

    @staticmethod
    def format_erddap_date(date: datetime) -> str:
        """Format date for ERDDAP API."""
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        return date.strftime('%Y-%m-%dT00:00:00Z')

    def file_date(self, date: datetime) -> str:
        """Format date for filenames."""
        return date.strftime("%Y%m%d")
