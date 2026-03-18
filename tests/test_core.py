"""Unit tests for the CleverPush tap."""

from datetime import datetime, timezone

from tap_cleverpush import streams as stream_defs
from tap_cleverpush.tap import TapCleverPush


def test_tap_discovers_expected_streams() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_names = {stream.name for stream in tap.discover_streams()}

    assert stream_names == {
        "channels",
        "notifications",
        "notification_hourly_statistics",
        "subscriptions",
        "subscription_count",
        "subscription_count_snapshots",
        "tags",
    }


def test_tap_notifications_replication_key() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}

    assert stream_map["notifications"].replication_key == "queued_at"


def test_tap_subscriptions_replication_key() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}

    assert stream_map["subscriptions"].replication_key == "syncedAt"


def test_tap_config_schema_fields() -> None:
    properties = TapCleverPush.config_jsonschema["properties"]

    assert "api_key" in properties
    assert "notifications_lookback_days" in properties
    assert properties["notifications_lookback_days"]["default"] == 3


def test_notifications_url_params_include_default_lookback(monkeypatch) -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["notifications"]
    start_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)

    monkeypatch.setattr(stream, "get_starting_timestamp", lambda context: start_ts)

    params = stream.get_url_params({"id": "channel-id"}, None)

    assert params["limit"] == stream.page_size
    assert params["offset"] == 0
    assert params["startDate"] == int(datetime(2024, 1, 7, tzinfo=timezone.utc).timestamp())


def test_notifications_url_params_apply_lookback_days(monkeypatch) -> None:
    tap = TapCleverPush(
        config={
            "api_key": "test-token",
            "notifications_lookback_days": 3,
        }
    )
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["notifications"]
    start_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)

    monkeypatch.setattr(stream, "get_starting_timestamp", lambda context: start_ts)
    params = stream.get_url_params({"id": "channel-id"}, None)

    assert params["startDate"] == int(datetime(2024, 1, 7, tzinfo=timezone.utc).timestamp())


def test_notifications_lookback_does_not_precede_config_start_date(monkeypatch) -> None:
    tap = TapCleverPush(
        config={
            "api_key": "test-token",
            "start_date": "2024-01-01T00:00:00Z",
            "notifications_lookback_days": 5,
        }
    )
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["notifications"]
    start_ts = datetime(2024, 1, 2, tzinfo=timezone.utc)

    monkeypatch.setattr(stream, "get_starting_timestamp", lambda context: start_ts)
    params = stream.get_url_params({"id": "channel-id"}, None)

    assert params["startDate"] == int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())


def test_subscriptions_url_params_include_updated_since(monkeypatch) -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscriptions"]
    start_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)

    monkeypatch.setattr(stream, "get_starting_timestamp", lambda context: start_ts)
    params = stream.get_url_params({"id": "channel-id"}, None)

    assert params["updatedSince"] == int(start_ts.timestamp() * 1000)


def test_subscriptions_schema_is_strict_and_fields_align() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscriptions"]

    params = stream.get_url_params({"id": "channel-id"}, None)
    assert "fields" in params

    assert set(params["fields"]) == {
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
    }

    schema_properties = set(stream.schema["properties"].keys())
    expected_requested_fields = schema_properties - {"id", "channel_id"}

    assert stream.schema["additionalProperties"] is False
    assert set(params["fields"]) == expected_requested_fields


def test_all_stream_schemas_are_strict() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})

    for stream in tap.discover_streams():
        assert stream.schema["additionalProperties"] is False


def test_subscriptions_post_process_falls_back_to_created_at() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscriptions"]

    row = stream.post_process({"_id": "abc123", "createdAt": "2024-01-10T00:00:00Z"})

    assert row is not None
    assert row["id"] == "abc123"
    assert row["syncedAt"] == "2024-01-10T00:00:00Z"


def test_subscriptions_post_process_drops_missing_synced_at_and_created_at() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscriptions"]

    row = stream.post_process({"_id": "abc123"})

    assert row is None


def test_post_process_drops_records_missing_primary_keys(caplog) -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["channels"]

    with caplog.at_level("WARNING"):
        row = stream.post_process({"name": "missing-id"})

    assert row is None
    assert "missing primary key fields" in caplog.text


def test_post_process_normalizes_id_from__id() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["channels"]

    row = stream.post_process({"_id": "abc123"})

    assert row is not None
    assert row["id"] == "abc123"


def test_channels_get_records_applies_allowlist_to__id(monkeypatch) -> None:
    tap = TapCleverPush(
        config={
            "api_key": "test-token",
            "channel_ids": ["keep-id"],
        }
    )
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["channels"]

    def fake_parent_get_records(self, context):  # noqa: ANN001
        yield {"_id": "keep-id"}
        yield {"_id": "drop-id"}

    monkeypatch.setattr(stream_defs.CleverPushStream, "get_records", fake_parent_get_records)

    records = list(stream.get_records(None))

    assert records == [{"_id": "keep-id"}]


def test_channels_post_process_strips_sensitive_fields() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["channels"]

    row = stream.post_process(
        {
            "_id": "abc123",
            "name": "example",
            "vapidPrivateKey": "secret",
            "fcmCredentials": "very-secret",
            "apnsAuthKey": "super-secret",
            "somePrivateKey": "other-secret",
        }
    )

    assert row is not None
    assert row["id"] == "abc123"
    assert "vapidPrivateKey" not in row
    assert "fcmCredentials" not in row
    assert "apnsAuthKey" not in row
    assert "somePrivateKey" not in row


def test_notifications_cutoff_is_timezone_aware(monkeypatch) -> None:
    tap = TapCleverPush(
        config={
            "api_key": "test-token",
            "notifications_lookback_days": 0,
        }
    )
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["notifications"]
    start_ts = datetime(2026, 2, 20, 0, 0, 0, tzinfo=timezone.utc)

    def fake_parent_get_records(self, context):  # noqa: ANN001
        yield {"_id": "newer", "queuedAt": "2026-02-20T00:05:00Z"}
        # This is 2026-02-19T23:00:00Z and should be treated as older.
        yield {"_id": "older-offset", "queuedAt": "2026-02-20T00:00:00+01:00"}
        yield {"_id": "should-not-be-read", "queuedAt": "2026-02-20T12:00:00Z"}

    monkeypatch.setattr(stream, "get_starting_timestamp", lambda context: start_ts)
    monkeypatch.setattr(stream_defs.CleverPushStream, "get_records", fake_parent_get_records)

    records = list(stream.get_records({"id": "channel-id"}))

    assert [record["_id"] for record in records] == ["newer"]


def test_notifications_child_context_provides_hourly_stats_path_params() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["notifications"]

    child_context = stream.get_child_context(
        {"_id": "notification-id"},
        {"id": "channel-id"},
    )

    assert child_context == {"id": "notification-id", "channelId": "channel-id"}


def test_notification_hourly_statistics_post_process_uses_context_notification_id() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["notification_hourly_statistics"]

    row = stream.post_process(
        {
            "_id": "hourly-row-id",
            "date": "2026-02-25T00:00:00Z",
            "clicked": 10,
        },
        {"id": "notification-id", "channelId": "channel-id"},
    )

    assert row is not None
    assert row["notification_id"] == "notification-id"
    assert row["channel_id"] == "channel-id"


def test_subscription_count_post_process_uses_channel_context_pk() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscription_count"]

    row = stream.post_process(
        {"subscriptions": 123, "inactiveSubscriptions": 4},
        {"id": "channel-id"},
    )

    assert row is not None
    assert row["channel_id"] == "channel-id"


def test_subscription_count_snapshots_schema_and_primary_key() -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscription_count_snapshots"]

    assert stream.primary_keys == ("channel_id", "snapshot_at")
    assert "snapshot_at" in stream.schema["properties"]
    assert stream.schema["additionalProperties"] is False


def test_subscription_count_snapshots_snapshot_at_is_constant_per_run(monkeypatch) -> None:
    tap = TapCleverPush(config={"api_key": "test-token"})
    stream_map = {stream.name: stream for stream in tap.discover_streams()}
    stream = stream_map["subscription_count_snapshots"]
    expected_snapshot_at = "2026-03-10T12:34:56.000Z"

    class FakeDatetime:
        calls = 0

        @classmethod
        def now(cls, tz):  # noqa: ANN001
            assert tz is timezone.utc
            cls.calls += 1
            return datetime(2026, 3, 10, 12, 34, 56, tzinfo=timezone.utc)

    monkeypatch.setattr(stream_defs, "datetime", FakeDatetime)

    row_one = stream.post_process(
        {"subscriptions": 123, "inactiveSubscriptions": 4},
        {"id": "channel-a"},
    )
    row_two = stream.post_process(
        {"subscriptions": 456, "inactiveSubscriptions": 7},
        {"id": "channel-b"},
    )

    assert row_one is not None
    assert row_two is not None
    assert row_one["snapshot_at"] == expected_snapshot_at
    assert row_two["snapshot_at"] == expected_snapshot_at
    assert FakeDatetime.calls == 1
