"""REST client handling, including CleverPushStream base class."""

from __future__ import annotations

import decimal
import sys
from typing import Any, ClassVar

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator, BaseOffsetPaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from collections.abc import Iterable

import requests
from singer_sdk.helpers.types import Context


class CleverPushOffsetPaginator(BaseOffsetPaginator):
    """Offset paginator for CleverPush list endpoints."""

    def __init__(self, records_key: str, page_size: int) -> None:
        super().__init__(start_value=0, page_size=page_size)
        self._records_key = records_key

    @override
    def has_more(self, response: requests.Response) -> bool:
        payload = response.json()
        records = payload.get(self._records_key, [])
        return isinstance(records, list) and len(records) >= self._page_size


class CleverPushCountOffsetPaginator(CleverPushOffsetPaginator):
    """Offset paginator for endpoints which also return a total count."""

    def __init__(
        self,
        records_key: str,
        page_size: int,
        count_key: str = "count",
    ) -> None:
        super().__init__(records_key=records_key, page_size=page_size)
        self._count_key = count_key

    @override
    def has_more(self, response: requests.Response) -> bool:
        payload = response.json()
        total_count = payload.get(self._count_key)
        if isinstance(total_count, str):
            try:
                total_count = int(total_count)
            except ValueError:
                pass
        if isinstance(total_count, int):
            return self.current_value + self._page_size < total_count
        return super().has_more(response)


class CleverPushSubscriptionCursorPaginator(BaseAPIPaginator[str | None]):
    """Cursor paginator for the subscriptions endpoint (startId-based).

    The subscriptions API does not support limit/offset. Pagination is done
    by passing the last record's _id as startId on each subsequent request.
    """

    def __init__(self, records_key: str) -> None:
        super().__init__(start_value=None)
        self._records_key = records_key

    @override
    def has_more(self, response: requests.Response) -> bool:
        records = response.json().get(self._records_key, [])
        return isinstance(records, list) and len(records) > 0

    @override
    def get_next(self, response: requests.Response) -> str | None:
        records = response.json().get(self._records_key, [])
        if not records:
            return None
        last = records[-1]
        return last.get("_id") or last.get("id")


class CleverPushStream(RESTStream):
    """CleverPush stream class."""

    schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "additionalProperties": True,
    }
    records_key: ClassVar[str | None] = None
    enforce_primary_key: ClassVar[bool] = True

    @override
    @property
    def url_base(self) -> str:
        return "https://api.cleverpush.com"

    @override
    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return an API key authenticator."""
        return APIKeyAuthenticator(
            key="Authorization",
            value=self.config["api_key"],
            location="header",
        )

    @property
    @override
    def http_headers(self) -> dict[str, str]:
        return {"User-Agent": "tap-cleverpush"}

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return URL query parameters."""
        return {}

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        payload = response.json(parse_float=decimal.Decimal)
        records: list[dict[str, Any]] | dict[str, Any] | None

        if self.records_key:
            if not isinstance(payload, dict):
                return
            records = payload.get(self.records_key)
        else:
            records = payload

        if records is None:
            return
        if isinstance(records, list):
            for record in records:
                if isinstance(record, dict):
                    yield record
            return
        if isinstance(records, dict):
            yield records

    @override
    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Normalize common identifiers across CleverPush endpoints."""
        # most cleverpush endpoints return _id (mongodb style), not id — normalize to id
        if "_id" in row:
            row.setdefault("id", row.pop("_id"))

        if context:
            channel_id = context.get("channelId") or context.get("id")
            if channel_id and "channel_id" not in row:
                row["channel_id"] = channel_id

        if self.enforce_primary_key and self.primary_keys:
            missing_keys = [key for key in self.primary_keys if row.get(key) in (None, "")]
            if missing_keys:
                self.logger.warning(
                    "Dropping record in stream %s due to missing primary key fields: %s",
                    self.name,
                    ", ".join(missing_keys),
                )
                return None

        if self.schema.get("additionalProperties") is False:
            allowed = set(self.schema.get("properties", {}).keys())
            row = {k: v for k, v in row.items() if k in allowed}

        return row
