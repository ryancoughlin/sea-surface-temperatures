from datetime import datetime, timedelta
from typing import Dict

class DateFormatter:
    def erddap_date(self, date: datetime, dataset: Dict) -> str:
        """Format date for ERDDAP URL."""
        return date.strftime(dataset['time_format'])
    
    def adjust_for_lag(self, date: datetime, dataset: Dict) -> datetime:
        """Adjust date based on data lag."""
        lag_days = dataset.get('lag_days', 0)
        return date - timedelta(days=lag_days)
    
    def file_date(self, date: datetime) -> str:
        """Format date for filenames."""
        return date.strftime("%Y%m%d")
