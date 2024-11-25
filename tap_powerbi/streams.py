from typing import Any, Optional, Iterable
from singer_sdk import typing as th
from tap_powerbi.client import PowerBIStream
import requests

class ReportsStream(PowerBIStream):
    """Stream for fetching reports."""

    name = "reports"
    path = "/groups/{workspace_id}/reports" 
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("webUrl", th.StringType),
        th.Property("embedUrl", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Fetch and filter the target report."""
        for record in super().get_records(context):
            if record["name"] == self.config["report"]:  # Match the target report
                yield record

class ReportDataSetDataStream(PowerBIStream):
    """Stream for fetching data from datasets associated with reports."""

    name = "report_dataset_data"
    path = "/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"  # Include workspace and dataset
    rest_method = "POST"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.results.[*].tables.[*]"
    parent_stream_type = ReportsStream

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("dataset_name", th.StringType),
        th.Property("rows", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the query payload for the target table."""
        return {
            "queries": [
                {
                    "query": f"EVALUATE Values('{context.get('table_name')}')",
                }
            ],
            "serializerSettings": {"includeNulls": True},
        }

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Fetch data only for the specified tables."""
        for table in self.config["tables"]:  # Use specified tables
            context["table_name"] = table
            yield from super().get_records(context)

class DataSetsStream(PowerBIStream):
    """Stream for fetching datasets."""

    name = "datasets"
    path = "/groups/{workspace_id}/datasets" 
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.value[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("webUrl", th.StringType),
        th.Property("configuredBy", th.StringType),
        th.Property("isRefreshable", th.BooleanType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Fetch and filter the target dataset."""
        for record in super().get_records(context):
            if record["id"] == self.config["dataset"]:
                yield record


class DataSetDataStream(PowerBIStream):
    """Stream for fetching data from a dataset."""

    name = "dataset_data"
    path = "/groups/{workspace_id}/datasets/{dataset_id}/executeQueries" 
    rest_method = "POST"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.results.[*].tables.[*]"

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("dataset_name", th.StringType),
        th.Property("rows", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Fetch data only for the specified tables."""
        for table in self.get_tables(context):
            if table in self.config["tables"]:
                self.current_table = table
                yield from super().get_records(context)

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the query payload for the target table."""
        return {
            "queries": [
                {
                    "query": f"EVALUATE Values('{self.current_table}')",
                }
            ],
            "serializerSettings": {"includeNulls": True},
        }

    def get_tables(self, context: Optional[dict]) -> list:
        """Retrieve tables for the target dataset."""
        if self.config.get("tables"):
            tables = self.config["tables"]
            if isinstance(tables, str):
                tables = tables.split(",")
            return tables
        return []


class ReportDataSetsStream(DataSetsStream):
    """Stream for datasets related to reports."""

    name = "report_datasets"
    path = "/groups/{workspace_id}/datasets/{dataset_id}"
    parent_stream_type = ReportsStream
    records_jsonpath = "$[*]"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Fetch datasets only for the specified report."""
        for record in super().get_records(context):
            if record["id"] == self.config["dataset"]:
                yield record
