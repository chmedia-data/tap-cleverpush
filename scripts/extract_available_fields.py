#!/usr/bin/env python3
"""Extract available fields per stream endpoint for schema expansion review.

This script compares three sources for each stream:
1) Current tap schema fields.
2) Swagger-documented response fields.
3) Live observed response fields from the CleverPush API.

Output is written as JSON to make future "what can we add?" reviews easy.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import Any

import requests

from tap_cleverpush.tap import TapCleverPush


@dataclass(frozen=True)
class StreamSpec:
    """Endpoint definition for field-inventory extraction."""

    name: str
    path: str
    method: str
    records_key: str | None


STREAM_SPECS: tuple[StreamSpec, ...] = (
    StreamSpec("channels", "/channels", "get", "channels"),
    StreamSpec("notifications", "/channel/{id}/notifications", "get", "notifications"),
    StreamSpec(
        "notification_hourly_statistics",
        "/channel/{channelId}/notification/{id}/hourly-statistics",
        "get",
        "statistics",
    ),
    StreamSpec("subscriptions", "/channel/{id}/subscriptions", "get", "subscriptions"),
    StreamSpec("subscription_count", "/channel/{id}/subscription-count", "get", None),
    StreamSpec("tags", "/channel/{id}/tags", "get", "tags"),
)

GLOBAL_FIELD_ALIASES: dict[str, str] = {"_id": "id", "queuedAt": "queued_at"}
STREAM_FIELD_ALIASES: dict[str, dict[str, str]] = {
    "notification_hourly_statistics": {"notification": "notification_id"},
}
DEFAULT_SWAGGER_URL = "https://api.cleverpush.com/swagger.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base-url", default="https://api.cleverpush.com")
    parser.add_argument(
        "--api-key", default=os.getenv("TAP_CLEVERPUSH_API_KEY") or os.getenv("API_KEY")
    )
    parser.add_argument(
        "--swagger-url",
        default=DEFAULT_SWAGGER_URL,
        help="Live Swagger URL to read when --swagger-path is not provided.",
    )
    parser.add_argument(
        "--swagger-path",
        type=Path,
        help="Optional local Swagger JSON snapshot. Overrides --swagger-url when set.",
    )
    parser.add_argument("--output", type=Path, default=Path("docs/available-fields-by-stream.json"))
    parser.add_argument("--max-channels", type=int, default=10)
    parser.add_argument("--max-records-per-stream", type=int, default=2000)
    parser.add_argument("--max-hourly-notifications-per-channel", type=int, default=25)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    return parser.parse_args()


# --- Extraction Helpers ---


def _get_records(payload: Any, records_key: str | None) -> list[dict[str, Any]]:
    """Safely extract a list of dictionaries from an API payload."""
    if not isinstance(payload, dict):
        return []
    data = payload.get(records_key) if records_key else payload
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    return []


def _extract_paths(value: Any, prefix: str = "") -> set[str]:
    """Recursively extract property paths from an object or schema."""
    paths: set[str] = set()
    if isinstance(value, dict):
        for key, nested in value.items():
            if not isinstance(key, str):
                continue
            path = f"{prefix}.{key}" if prefix else key
            paths.add(path)
            paths.update(_extract_paths(nested, prefix=path))
    elif isinstance(value, list):
        array_prefix = f"{prefix}[]" if prefix else "[]"
        for nested in value:
            paths.update(_extract_paths(nested, prefix=array_prefix))
    return paths


def _extract_top_level_fields(records: Iterable[dict[str, Any]]) -> set[str]:
    return {key for record in records for key in record if isinstance(key, str)}


# --- Swagger Logic ---


def _load_swagger_fields(swagger: dict[str, Any], spec: StreamSpec) -> tuple[set[str], set[str]]:
    try:
        schema = swagger["paths"][spec.path][spec.method]["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
    except KeyError:
        return set(), set()

    # Unwrap array/container schemas
    if spec.records_key and isinstance(schema.get("properties", {}).get(spec.records_key), dict):
        schema = schema["properties"][spec.records_key]
    if schema.get("type") == "array" and isinstance(schema.get("items"), dict):
        schema = schema["items"]

    properties = schema.get("properties", {})
    top_level = {k for k in properties if isinstance(k, str)}

    # Adapt schema structure for generalized path extractor
    return top_level, _extract_paths(schema.get("properties", {}))


def _extract_subscriptions_extra_fields(swagger: dict[str, Any]) -> list[str]:
    default = [
        "customAttributes",
        "tags",
        "topics",
        "segments",
        "country",
        "language",
        "createdAt",
        "clickedDates",
    ]
    try:
        params = swagger["paths"]["/channel/{id}/subscriptions"]["get"]["parameters"]
        for param in params:
            if param.get("name") == "fields" and (desc := param.get("description")):
                if match := re.search(r"can contain:\s*(.+?)\.", desc, flags=re.IGNORECASE):
                    return [i.strip() for i in match.group(1).split(",") if i.strip()] or default
    except KeyError:
        pass
    return default


def _load_swagger_document(
    *,
    swagger_path: Path | None,
    swagger_url: str,
    timeout_seconds: int,
) -> tuple[dict[str, Any], str]:
    """Load the Swagger document from a local snapshot or live URL."""
    if swagger_path is not None:
        if not swagger_path.exists():
            raise SystemExit(f"Swagger file not found: {swagger_path}")
        with swagger_path.open("r", encoding="utf-8") as file:
            return json.load(file), str(swagger_path)

    try:
        response = requests.get(swagger_url, timeout=timeout_seconds)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SystemExit(f"Failed to fetch Swagger from {swagger_url}: {exc}") from exc

    return response.json(), swagger_url


# --- API Logic ---


class ApiClient:
    def __init__(self, base_url: str, api_key: str, timeout: int):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": api_key, "User-Agent": "tap-field-inventory"})

    def get(self, path: str, params: dict | None = None) -> Any:
        response = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()


def _yield_stream_records(
    client: ApiClient,
    spec: StreamSpec,
    channel_ids: list[str],
    notifications_by_chan: dict[str, list[str]],
    sub_fields: list[str],
    max_hourly: int,
) -> Iterator[dict[str, Any]]:
    """Generates records for a given stream, handling custom pagination per endpoint."""
    for cid in channel_ids:
        if spec.name == "subscription_count":
            yield from _get_records(
                client.get(f"/channel/{cid}/subscription-count"), spec.records_key
            )

        elif spec.name == "notifications":
            offset, limit = 0, 500
            while True:
                records = _get_records(
                    client.get(
                        f"/channel/{cid}/notifications", params={"limit": limit, "offset": offset}
                    ),
                    spec.records_key,
                )
                if not records:
                    break
                for r in records:
                    if r_id := r.get("_id") or r.get("id"):
                        notifications_by_chan.setdefault(cid, []).append(str(r_id))
                    yield r
                if len(records) < limit:
                    break
                offset += limit

        elif spec.name == "notification_hourly_statistics":
            for nid in notifications_by_chan.get(cid, [])[:max_hourly]:
                yield from _get_records(
                    client.get(f"/channel/{cid}/notification/{nid}/hourly-statistics"),
                    spec.records_key,
                )

        elif spec.name == "subscriptions":
            start_id = None
            while True:
                params = (
                    {"fields": sub_fields, "startId": start_id}
                    if start_id
                    else {"fields": sub_fields}
                )
                records = _get_records(
                    client.get(f"/channel/{cid}/subscriptions", params=params), spec.records_key
                )
                if not records:
                    break
                yield from records

                next_id = records[-1].get("_id") or records[-1].get("id")
                if not next_id or next_id == start_id:
                    break
                start_id = next_id

        elif spec.name == "tags":
            skip, limit = 0, 100
            while True:
                records = _get_records(
                    client.get(f"/channel/{cid}/tags", params={"limit": limit, "skip": skip}),
                    spec.records_key,
                )
                if not records:
                    break
                yield from records
                if len(records) < limit:
                    break
                skip += limit


# --- Core Process ---


def _build_stream_output(
    spec: StreamSpec,
    schema_fields: set[str],
    swagger_fields: set[str],
    swagger_paths: set[str],
    observed_records: list[dict],
    live_error: str | None,
) -> dict:
    observed_top_level = _extract_top_level_fields(observed_records)
    observed_paths = set().union(*[_extract_paths(r) for r in observed_records])

    available_fields = swagger_fields | observed_top_level
    missing_raw = available_fields - schema_fields

    alias_suppressed = {
        f
        for f in missing_raw
        if STREAM_FIELD_ALIASES.get(spec.name, {}).get(f) in schema_fields
        or GLOBAL_FIELD_ALIASES.get(f) in schema_fields
    }

    return {
        "endpoint": spec.path,
        "method": spec.method.upper(),
        "records_key": spec.records_key,
        "schema_fields": sorted(schema_fields),
        "swagger_fields": sorted(swagger_fields),
        "observed_fields": sorted(observed_top_level),
        "available_fields": sorted(available_fields),
        "missing_from_schema": sorted(missing_raw - alias_suppressed),
        "alias_suppressed_from_missing": sorted(alias_suppressed),
        "schema_only": sorted(schema_fields - available_fields),
        "swagger_field_paths": sorted(swagger_paths),
        "observed_field_paths": sorted(observed_paths),
        "observed_record_count": len(observed_records),
        "live_error": live_error,
    }


def main() -> None:
    args = _parse_args()
    swagger, swagger_source = _load_swagger_document(
        swagger_path=args.swagger_path,
        swagger_url=args.swagger_url,
        timeout_seconds=args.timeout_seconds,
    )

    tap = TapCleverPush(config={"api_key": args.api_key or "placeholder"})
    stream_map = {s.name: s for s in tap.discover_streams()}
    specs = list(STREAM_SPECS)
    live_records: dict[str, list[dict]] = {s.name: [] for s in specs}
    live_errors: dict[str, str] = {}

    if args.api_key:
        client = ApiClient(args.api_base_url, args.api_key, args.timeout_seconds)
        sub_fields = _extract_subscriptions_extra_fields(swagger)
        notifications_by_chan: dict[str, list[str]] = {}

        try:
            # Seed channels
            chan_payload = client.get("/channels")
            channels = _get_records(chan_payload, "channels")[: args.max_channels]
            live_records["channels"] = channels[: args.max_records_per_stream]
            channel_ids = [
                str(c.get("_id") or c.get("id")) for c in channels if c.get("_id") or c.get("id")
            ]

            for spec in specs:
                if spec.name == "channels":
                    continue
                try:
                    # 'islice' cleanly handles the max_records limit, letting generators be simple while loops
                    record_generator = _yield_stream_records(
                        client,
                        spec,
                        channel_ids,
                        notifications_by_chan,
                        sub_fields,
                        args.max_hourly_notifications_per_channel,
                    )
                    live_records[spec.name] = list(
                        islice(record_generator, args.max_records_per_stream)
                    )
                except requests.RequestException as exc:
                    live_errors[spec.name] = str(exc)
        except requests.RequestException as exc:
            live_errors = {s.name: f"/channels request failed: {exc}" for s in specs}

    output_streams = {}
    for spec in specs:
        schema_props = stream_map.get(spec.name, tap).schema.get("properties", {})
        schema_fields = {k for k in schema_props if isinstance(k, str)}
        swagger_fields, swagger_paths = _load_swagger_fields(swagger, spec)

        output_streams[spec.name] = _build_stream_output(
            spec,
            schema_fields,
            swagger_fields,
            swagger_paths,
            live_records[spec.name],
            live_errors.get(spec.name),
        )

    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "api_base_url": args.api_base_url,
        "swagger_source": swagger_source,
        "used_live_api": bool(args.api_key),
        "max_channels": args.max_channels,
        "max_records_per_stream": args.max_records_per_stream,
        "max_hourly_notifications_per_channel": args.max_hourly_notifications_per_channel,
        "streams": output_streams,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, sort_keys=True)
        file.write("\n")

    print(f"Wrote field inventory to: {args.output}")
    for stream_name, stream_data in sorted(output_streams.items()):
        print(
            f"- {stream_name}: observed_records={stream_data['observed_record_count']}, "
            f"missing_from_schema={len(stream_data['missing_from_schema'])}"
        )


if __name__ == "__main__":
    main()
