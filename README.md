# tap-cleverpush

`tap-cleverpush` is a Singer tap for the CleverPush REST API.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## API References

- CleverPush Swagger: [https://api.cleverpush.com/swagger.json](https://api.cleverpush.com/swagger.json)

This tap is implemented against the live CleverPush API, not the Swagger alone.

Treat the Swagger as approximate. It is useful for endpoint discovery and field hints, but it diverges from observed API behavior in a few places:

- documented fields are sometimes absent in live responses
- live responses sometimes contain fields not described in the Swagger
- pagination and filter behavior in practice does not always match the documented contract

When the Swagger conflicts with observed payloads or endpoint behavior, prefer the tap implementation and live API output.

## Installation

This repository includes a `justfile` for common workflows.

Install `just` if needed:

```bash
brew install just
```

Install the tap from this repository:

```bash
just install
```

For development:

```bash
just sync
```

## Configuration

### Accepted Config Options

Key settings:

- `api_key` (required): CleverPush private API key
- `channel_ids` (optional): allowlist of channel IDs
- `start_date` (optional): earliest timestamp for incremental extraction
- `notifications_lookback_days` (optional, default `3`): re-fetch recent notifications to refresh changing metrics

For the full generated settings schema, run:

```bash
just about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

Example:

```bash
cat > .env <<'EOF'
TAP_CLEVERPUSH_API_KEY=your_api_key_here
TAP_CLEVERPUSH_CHANNEL_IDS=channel1,channel2
TAP_CLEVERPUSH_START_DATE=2024-01-01T00:00:00Z
TAP_CLEVERPUSH_NOTIFICATIONS_LOOKBACK_DAYS=7
EOF

just discover
```

### Source Authentication and Authorization

Set `api_key` to a CleverPush private API key. The tap sends it in the
`Authorization` request header.

## Stream Fetch Reference

### Default Stream Graph

```text
channels (full_table)
├── notifications (incremental, cursor=queued_at)
│   └── notification_hourly_statistics (full_table per notification)
├── subscriptions (incremental, cursor=syncedAt)
├── subscription_count (full_table snapshot)
├── subscription_count_snapshots (full_table snapshot per run)
└── tags (full_table)
```

### Stream Contracts

| Stream | Parent | Endpoint | Mode | Cursor | Request window/filter | Pagination |
|---|---|---|---|---|---|---|
| `channels` | None | `/channels` | Full-table | None | Optional `channel_ids` allowlist after fetch | `SinglePagePaginator` |
| `notifications` | `channels` | `/channel/{id}/notifications` | Incremental | `queued_at` | `startDate = effective_start` (unix seconds) | Offset (`limit`, `offset`) |
| `notification_hourly_statistics` | `notifications` | `/channel/{channelId}/notification/{id}/hourly-statistics` | Full-table per notification | None | Fetched for notifications returned in the same run | `SinglePagePaginator` |
| `subscriptions` | `channels` | `/channel/{id}/subscriptions` | Incremental | `syncedAt` | `updatedSince` (unix millis) | Cursor (`startId`) |
| `subscription_count` | `channels` | `/channel/{id}/subscription-count` | Full-table snapshot | None | None | `SinglePagePaginator` |
| `subscription_count_snapshots` | `channels` | `/channel/{id}/subscription-count` | Full-table snapshot per run | None | `snapshot_at` is assigned once per sync run | `SinglePagePaginator` |
| `tags` | `channels` | `/channel/{id}/tags` | Full-table | None | None | Offset (`limit`, `skip`) |

### Selected Top-Level Fields

| Stream | Top-level fields |
|---|---|
| `channels` | `id`, `name`, `identifier`, `domain`, `type`, `project`, `createdAt`, `updatedAt`, `optIns`, `subscriptions`, `inactiveSubscriptions`, `weeklyOptIns`, `weeklyOptInsDesktop`, `weeklyOptInsMobile`, `industry`, `ownDomain`, `isChannelNew`, `markedForDeletion` |
| `notifications` | `id`, `channel_id`, `text`, `status`, `url`, `createdAt`, `queued_at`, `clicked`, `delivered`, `optOuts`, `source`, `sentAt`, `subscriptionCount`, `transactional`, `isTestNotification`, `inactiveSubscriptionCount`, `errorCount` |
| `notification_hourly_statistics` | `id`, `notification_id`, `channel_id`, `channelId`, `date`, `delivered`, `clicked` |
| `subscriptions` | `id`, `channel`, `channel_id`, `type`, `inactive`, `country`, `language`, `platformName`, `platformVersion`, `browserType`, `browserVersion`, `timezone`, `topics`, `createdAt`, `syncedAt` |
| `subscription_count` | `id`, `channel_id`, `subscriptions`, `inactiveSubscriptions` |
| `subscription_count_snapshots` | `id`, `channel_id`, `subscriptions`, `inactiveSubscriptions`, `snapshot_at` |
| `tags` | `id`, `channel_id`, `name`, `createdAt`, `inactiveSubscriptions`, `subscriptions`, `tagGroups` |

### Field Inventory Helper

To review live and Swagger-available fields per endpoint and compare against current schema:

```bash
just field-inventory
```

`just field-inventory` reads the live Swagger from `https://api.cleverpush.com/swagger.json` by default. Use `--swagger-path /path/to/swagger.json` if you want to compare against a local snapshot instead.


## Usage

You can easily run `tap-cleverpush` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
just version
just help
just discover CONFIG
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
just sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
just test
```

You can also test the `tap-cleverpush` CLI interface directly:

```bash
just help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Use Meltano to run an EL pipeline:

```bash
# Install meltano
just meltano-install

# Test invocation
just meltano-invoke-version

# Run a test EL pipeline
just meltano-run
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
