from datetime import datetime, UTC
from typing import SupportsIndex

def get_datetime_now() -> datetime:
    """ Retorna el tiempo actual en UTC."""
    return datetime.now(tz=UTC)

def get_datetime(
        year: SupportsIndex,
        month: SupportsIndex,
        day: SupportsIndex,
        hour: SupportsIndex = 0,
        minute: SupportsIndex = 0,
        second: SupportsIndex = 0,
        microsecond: SupportsIndex = 0,
    ) -> datetime:
    date = datetime(year, month, day, hour, minute, second, microsecond, tzinfo=UTC)
    return date