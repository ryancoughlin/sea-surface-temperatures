from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

class DateFormatter:
    @staticmethod
    def get_current_date() -> datetime:
        """Get current date in UTC, ensuring we get today's date."""
        now = datetime.now(timezone.utc)
        # Ensure we're getting today's date in UTC
        return now.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def get_query_date(date: datetime, lag_days: int = 0) -> datetime:
        """Get standardized query date with lag days."""
        # Ensure date is in UTC
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        
        # Normalize to start of day and adjust for lag
        normalized_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        return normalized_date - timedelta(days=lag_days)

    @staticmethod
    def format_api_date(date: datetime) -> Tuple[str, str]:
        """Format date range for API requests."""
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        
        start = f"{date.strftime('%Y-%m-%d')}T00:00:00Z"
        end = f"{date.strftime('%Y-%m-%d')}T23:59:59Z"
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
