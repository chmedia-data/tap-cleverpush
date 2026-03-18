"""Stream type classes for tap-cleverpush."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar

from singer_sdk import typing as th

from tap_cleverpush.client import (
    CleverPushOffsetPaginator,
    CleverPushStream,
    CleverPushSubscriptionCursorPaginator,
)


def _schema(*properties: Any) -> dict[str, Any]:
    """Build a strict JSON Schema with explicit payload columns."""
    schema = th.PropertiesList(*properties).to_dict()
    # keep schemas strict so new upstream keys are visible as drift, not silently loaded.
    schema["additionalProperties"] = False
    return schema


_SENSITIVE_CHANNEL_FIELD_NAMES = {
    "apnsAuthKey",
    "fcmCredentials",
    "vapidPrivateKey",
}


def _format_utc_datetime(dt: datetime) -> str:
    """Format a UTC-aware datetime as a Z-suffixed ISO 8601 string."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _to_utc_datetime(value: Any) -> datetime | None:
    """Parse datetime-like values into UTC-aware datetimes."""
    if value is None:
        return None

    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        normalized = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class ChannelsStream(CleverPushStream):
    """All CleverPush channels (full-table due to missing update cursor)."""

    name = "channels"
    path = "/channels"
    primary_keys = ("id",)
    records_key = "channels"
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("identifier", th.StringType),
        th.Property("domain", th.StringType),
        th.Property("type", th.StringType),
        th.Property("project", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("updatedAt", th.DateTimeType),
        th.Property("optIns", th.NumberType),
        th.Property("subscriptions", th.NumberType),
        th.Property("inactiveSubscriptions", th.NumberType),
        th.Property("weeklyOptIns", th.NumberType),
        th.Property("weeklyOptInsDesktop", th.NumberType),
        th.Property("weeklyOptInsMobile", th.NumberType),
        th.Property("industry", th.StringType),
        th.Property("ownDomain", th.BooleanType),
        th.Property("isChannelNew", th.BooleanType),
        th.Property("markedForDeletion", th.BooleanType),
    )

    def get_records(self, context: dict | None) -> Iterable[dict]:
        channel_ids = self.config.get("channel_ids")
        if isinstance(channel_ids, str):
            allowed = {value.strip() for value in channel_ids.split(",") if value.strip()}
        else:
            allowed = set(channel_ids or [])

        for row in super().get_records(context):
            # /channels commonly returns _id before normalization to id.
            channel_id = row.get("id") or row.get("_id")
            if allowed and channel_id not in allowed:
                continue
            yield row

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {"id": record.get("id")}

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row = super().post_process(row, context)
        if row is None:
            return None
        self._remove_sensitive_channel_fields(row)
        return row

    def _remove_sensitive_channel_fields(self, row: dict[str, Any]) -> None:
        """Remove high-risk credentials from channel payload rows."""
        for key in list(row):
            if key in _SENSITIVE_CHANNEL_FIELD_NAMES:
                row.pop(key, None)


class _OffsetPagedStream(CleverPushStream):
    """Base stream for limit + offset/skip pagination patterns."""

    records_key: ClassVar[str]
    page_size: ClassVar[int] = 100
    offset_param: ClassVar[str] = "offset"

    def get_new_paginator(self) -> CleverPushOffsetPaginator:
        return CleverPushOffsetPaginator(records_key=self.records_key, page_size=self.page_size)

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        return {
            "limit": self.page_size,
            self.offset_param: next_page_token if next_page_token is not None else 0,
        }


class NotificationsStream(_OffsetPagedStream):
    """Channel notifications (incremental on queued_at)."""

    name = "notifications"
    parent_stream_type = ChannelsStream
    path = "/channel/{id}/notifications"
    primary_keys = ("id",)

    # queued_at is the best watermark for notification freshness.
    # createdAt alone can miss later metric/status changes to existing notifications.
    replication_key = "queued_at"

    records_key = "notifications"
    page_size = 500
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("channel_id", th.StringType),
        th.Property("text", th.StringType),
        th.Property("status", th.StringType),
        th.Property("url", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("queued_at", th.DateTimeType),
        th.Property("clicked", th.NumberType),
        th.Property("delivered", th.NumberType),
        th.Property("optOuts", th.NumberType),
        th.Property("source", th.StringType),
        th.Property("sentAt", th.DateTimeType),
        th.Property("subscriptionCount", th.NumberType),
        th.Property("transactional", th.BooleanType),
        th.Property("isTestNotification", th.BooleanType),
        th.Property("inactiveSubscriptionCount", th.NumberType),
        th.Property("errorCount", th.NumberType),
    )

    def _get_notifications_lookback_days(self) -> int:
        raw_value = self.config.get("notifications_lookback_days", 3)
        try:
            lookback_days = int(raw_value)
        except (TypeError, ValueError):
            self.logger.warning(
                "Invalid notifications_lookback_days=%r; defaulting to 3",
                raw_value,
            )
            return 3
        return max(0, lookback_days)

    def get_effective_start_timestamp(self, context: dict | None) -> datetime | None:
        base_start = _to_utc_datetime(self.get_starting_timestamp(context))
        if base_start is None:
            return None

        lookback_days = self._get_notifications_lookback_days()
        if lookback_days <= 0:
            return base_start

        lookback_start = base_start - timedelta(days=lookback_days)
        configured_start = _to_utc_datetime(self.config.get("start_date"))
        if configured_start is not None and lookback_start < configured_start:
            return configured_start
        return lookback_start

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        if start_ts := self.get_effective_start_timestamp(context):
            params["startDate"] = int(start_ts.timestamp())
        return params

    def get_records(self, context: dict | None) -> Iterable[dict]:
        # the api ignores the startDate query param and always returns all records
        # sorted newest first, so we stop pagination as soon as we see a record
        # older than the replication start timestamp
        start_ts = self.get_effective_start_timestamp(context)
        for record in super().get_records(context):
            queued = _to_utc_datetime(record.get("queuedAt") or record.get("createdAt"))
            if start_ts and queued and queued < start_ts:
                return
            yield record

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        channel_id = None
        if context:
            channel_id = context.get("id") or context.get("channelId")
        if channel_id is None:
            channel_id = record.get("channel_id") or record.get("channel")
        return {
            "id": record.get("id") or record.get("_id"),
            "channelId": channel_id,
        }

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        # The API emits queuedAt (camelCase), but incremental state uses queued_at.
        # Mirror queuedAt/createdAt into queued_at so bookmarking and comparisons are consistent.
        if "queued_at" not in row:
            row["queued_at"] = row.get("queuedAt") or row.get("createdAt")

        row = super().post_process(row, context)
        if row is None:
            return None
        return row


class NotificationHourlyStatisticsStream(CleverPushStream):
    """Hourly notification statistics per notification (snapshot by hour)."""

    name = "notification_hourly_statistics"
    parent_stream_type = NotificationsStream
    path = "/channel/{channelId}/notification/{id}/hourly-statistics"

    # business grain over source row ids for stable downstream dedupe.
    primary_keys = ("notification_id", "channel_id", "date")

    records_key = "statistics"
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("notification_id", th.StringType),
        th.Property("channel_id", th.StringType),
        th.Property("channelId", th.StringType),
        th.Property("date", th.StringType),
        th.Property("delivered", th.NumberType),
        th.Property("clicked", th.NumberType),
    )

    def parse_response(self, response: Any) -> Iterable[dict]:
        """Drop noisy transport keys before schema validation."""
        for row in super().parse_response(response):
            row.pop("_id", None)
            row.pop("id", None)
            row.pop("channelId", None)
            row.pop("notification", None)
            row.pop(
                "hour", None
            )  # Documented in Swagger but not consistently present in live responses.
            yield row

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        if not row.get("notification_id"):
            row["notification_id"] = row.get("notification")
        if not row.get("notification_id") and context:
            row["notification_id"] = context.get("id")
        row = super().post_process(row, context)
        if row is None:
            return None
        return row

class SubscriptionCountStream(CleverPushStream):
    """Current subscription counters per channel.

    Use SubscriptionCountSnapshotsStream for historical snapshot retention.
    """

    name = "subscription_count"
    parent_stream_type = ChannelsStream
    path = "/channel/{id}/subscription-count"
    primary_keys = ("channel_id",)
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("channel_id", th.StringType),
        th.Property("subscriptions", th.NumberType),
        th.Property("inactiveSubscriptions", th.NumberType),
    )


class SubscriptionCountSnapshotsStream(SubscriptionCountStream):
    """Historical subscription counter snapshots per channel."""

    name = "subscription_count_snapshots"
    primary_keys = ("channel_id", "snapshot_at")
    _snapshot_at: str | None = None
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("channel_id", th.StringType),
        th.Property("subscriptions", th.NumberType),
        th.Property("inactiveSubscriptions", th.NumberType),
        th.Property("snapshot_at", th.DateTimeType),
    )

    def _get_snapshot_at(self) -> str:
        if self._snapshot_at is None:
            self._snapshot_at = _format_utc_datetime(datetime.now(timezone.utc))
        return self._snapshot_at

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        row["snapshot_at"] = self._get_snapshot_at()
        row = super().post_process(row, context)
        if row is None:
            return None
        return row


class SubscriptionsStream(CleverPushStream):
    """Subscriptions per channel (incremental on syncedAt)."""

    name = "subscriptions"
    parent_stream_type = ChannelsStream
    path = "/channel/{id}/subscriptions"
    primary_keys = ("id",)

    # syncedAt tracks mutations; createdAt would miss updates on older subscriptions.
    replication_key = "syncedAt"

    records_key = "subscriptions"
    subscription_api_fields: ClassVar[tuple[str, ...]] = (
        # Explicit field list is required for this endpoint.
        # Without it, payloads can degrade to sparse/id-only responses.
        # segments is intentionally omitted from the default analytics footprint.
        "channel",
        "type",
        "inactive",
        "country",
        "language",
        "platformName",
        "platformVersion",
        "browserType",
        "browserVersion",
        "timezone",
        "topics",
        "createdAt",
        "syncedAt",
    )
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("channel", th.StringType),
        th.Property("channel_id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("inactive", th.BooleanType),
        th.Property("country", th.StringType),
        th.Property("language", th.StringType),
        th.Property("platformName", th.StringType),
        th.Property("platformVersion", th.StringType),
        th.Property("browserType", th.StringType),
        th.Property("browserVersion", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("topics", th.ArrayType(th.StringType)),
        th.Property("createdAt", th.DateTimeType),
        th.Property("syncedAt", th.DateTimeType),
    )

    def get_new_paginator(self) -> CleverPushSubscriptionCursorPaginator:
        return CleverPushSubscriptionCursorPaginator(records_key="subscriptions")

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "fields": list(self.subscription_api_fields),
        }
        if next_page_token is not None:
            params["startId"] = next_page_token
        if start_ts := self.get_starting_timestamp(context):
            params["updatedSince"] = int(start_ts.timestamp() * 1000)
        return params

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        if not row.get("syncedAt"):
            # Fallback keeps incremental state monotonic when syncedAt is omitted.
            row["syncedAt"] = row.get("createdAt")
            self.logger.warning(
                "createdAt not found in %s. Falling back to syncedAt",
                self.name,
            )

        if not row.get("syncedAt"):
            self.logger.warning(
                "Dropping record in stream %s due to missing syncedAt and createdAt",
                self.name,
            )
            return None

        row = super().post_process(row, context)
        if row is None:
            return None
        return row


class TagsStream(_OffsetPagedStream):
    """Tags per channel (full-table; no updatedSince filter)."""

    name = "tags"
    parent_stream_type = ChannelsStream
    path = "/channel/{id}/tags"
    primary_keys = ("id",)
    records_key = "tags"
    offset_param = "skip"
    page_size = 100
    schema = _schema(
        th.Property("id", th.StringType),
        th.Property("channel_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("inactiveSubscriptions", th.NumberType),
        th.Property("subscriptions", th.NumberType),
        th.Property("tagGroups", th.ArrayType(th.StringType)),
    )
