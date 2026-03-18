default:
    @just --list

install:
    uv tool install .

sync:
    uv sync

about:
    uv run tap-cleverpush --about

help:
    uv run tap-cleverpush --help

version:
    uv run tap-cleverpush --version

discover config="ENV" catalog="catalog.json":
    uv run tap-cleverpush --config {{config}} --discover > {{catalog}}

test:
    uv run pytest

test-match pattern:
    uv run pytest -k "{{pattern}}"

field-inventory output="docs/available-fields-by-stream.json":
    uv run python scripts/extract_available_fields.py --output {{output}}

meltano-install:
    uv tool install meltano

meltano-invoke-version:
    meltano invoke tap-cleverpush --version

meltano-run:
    meltano run tap-cleverpush target-jsonl
