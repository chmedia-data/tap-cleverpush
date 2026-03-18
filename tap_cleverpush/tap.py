"""CleverPush tap class."""

from __future__ import annotations

import sys

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_cleverpush import streams


class TapCleverPush(Tap):
    """Singer tap for CleverPush."""

    name = "tap-cleverpush"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="CleverPush private API key.",
        ),
        th.Property(
            "channel_ids",
            th.ArrayType(th.StringType(nullable=False), nullable=False),
            title="Channel IDs",
            description="Optional allowlist of channel IDs to replicate.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="Earliest timestamp for incremental streams.",
        ),
        th.Property(
            "notifications_lookback_days",
            th.IntegerType(nullable=False),
            description=(
                "Optional lookback window (days) applied to notifications incremental "
                "sync to re-fetch recent notifications for metric updates."
            ),
            default=3,
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.CleverPushStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ChannelsStream(self),
            # channel_detail is intentionally excluded by default to avoid
            # duplicating near-identical channel payloads in downstream models.
            streams.NotificationsStream(self),
            streams.NotificationHourlyStatisticsStream(self),
            streams.SubscriptionsStream(self),
            streams.SubscriptionCountStream(self),
            streams.SubscriptionCountSnapshotsStream(self),
            streams.TagsStream(self),
        ]


if __name__ == "__main__":
    TapCleverPush.cli()
