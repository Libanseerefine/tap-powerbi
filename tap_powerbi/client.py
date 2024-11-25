"""REST client handling, including PowerBIStream base class."""

from pathlib import Path
from typing import Any, Dict, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class PowerBIStream(RESTStream):
    """PowerBI stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    records_jsonpath = "$.value[*]" 
    next_page_token_jsonpath = "$.next_page"
    _page_size = 1000
    offset = 0

    @property
    def http_headers(self) -> dict:
        """Return the HTTP headers needed for the request."""
        headers = super().http_headers or {}
        if "token" in self.config:
            headers["Authorization"] = f"Bearer {self.config['token']}"
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        next_page_token = None
        if self.name == "dataset_data":
            self.offset += self._page_size
            all_matches = extract_jsonpath(self.records_jsonpath, response.json())
            first_match = next(iter(all_matches), None)
            if first_match is None:
                return None
            if "rows" in first_match:
                if len(first_match["rows"]) == 0:
                    return None
            else:
                return None
            return self.offset

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            data = response.json()
            if self.name == "dataset_data" and "error" in data:
                self.logger.warning(
                    f"Error fetching data for {response.request.url}, "
                    f"body {response.request.body}, response: {response.text}"
                )
            else:
                raise FatalAPIError(msg)