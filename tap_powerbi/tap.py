"""PowerBI tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_powerbi.streams import (
    DataSetDataStream,
    DataSetsStream,
    PowerBIStream,
    ReportsStream,
    ReportDataSetsStream,
    ReportDataSetDataStream,
)

STREAM_TYPES = [
    ReportsStream,
    DataSetsStream,
    DataSetDataStream,
    ReportDataSetsStream,
    ReportDataSetDataStream,
]


class TapPowerBI(Tap):
    """PowerBI tap class."""

    name = "tap-powerbi"

    config_jsonschema = th.PropertiesList(
        th.Property("token", th.StringType, required=True),  # API Token
        th.Property("workspace", th.StringType, required=True),  # Target a specific workspace
        th.Property("report", th.StringType, required=True),  # Target a single report
        th.Property("dataset", th.StringType, required=True),  # Target a single dataset
        th.Property("tables", th.CustomType({"type": ["array", "string"]}), required=True),  # List of tables
    ).to_dict()
    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapPowerBI.cli()
