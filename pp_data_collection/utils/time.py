import time
from datetime import datetime, timezone


def datetime_2_timestamp(dt: datetime, tz: int = 7) -> int:
    """
    Convert a datetime object to timestamp

    Args:
        dt: datetime object
        tz: timezone of the datetime object

    Returns:
        timestamp in millisecond
    """
    dt = dt.replace(tzinfo=timezone.utc)
    timestamp = dt.timestamp() - tz * 3600
    return round(timestamp * 1000)


def timestamp_2_datetime(timestamp: int, tz: int = 7) -> datetime:
    """
    Convert timestamp (millisecond) to a datetime object

    Args:
        timestamp: timestamp in millisecond
        tz: timezone to convert

    Returns:
        a datetime object
    """
    return datetime.utcfromtimestamp(timestamp / 1000 + tz * 3600)


class TimeThis:
    def __init__(self, op_name: str = 'operation', printer=print, **kwargs):
        """
        Measure running time of a block of code

        Args:
            op_name: just some text to print
            printer: print function
            **kwargs: keyword args for the print function
        """
        self.op_name = op_name
        self.printer = printer
        self.printer_kwargs = kwargs

    def __enter__(self):
        self.start_time = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.printer(f'Elapsed time for {self.op_name}: {time.time() - self.start_time}(s)', **self.printer_kwargs)
