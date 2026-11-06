# Agent trace exports

Export agent traces as GenAI OTLP JSONL, with one source database row per line.

Exports default to a **10-row sample from the last 24 hours**, unsorted, with a
5-minute query timeout. Samples go to stdout unless `--output PATH` is supplied.
Use `--limit N` to change the sample size, or `--no-sample` for a fuller file export
with explicit time bounds or `--all-time`. See [sampling behavior](docs/sampling.md).

- [Snowflake](snowflake/README.md): Cortex export and local GenAI conversion.
- [Databricks](databricks/README.md): MLflow unified trace export and local GenAI conversion.
- [ClickHouse](clickhouse/README.md): self-hosted Langfuse v4 `events_full` export and local GenAI conversion.
- [Pydantic Logfire](pydantic-logfire/README.md): `records` export through the Logfire query API, with resumable downloads.

Each exporter downloads raw rows to a temporary file, converts them locally, then
deletes the temporary download. Use `--keep-raw PATH` to retain it, or
`--format raw` for an unconverted export (one object per database record).
A standalone converter can reprocess saved raw exports without warehouse access.
Snowflake, ClickHouse and Logfire rows contain one span; Databricks unified rows contain a trace's spans.
Shared OTLP and message helpers live in `trace_export/`.
Local exports in `data/` and all `*.jsonl` files are gitignored.
The only JSONL exception is the synthetic examples under `examples/`.

Discover visible agents/tables with `snowflake/discover.py` or
`databricks/discover.py`, using `--connection '<profile_name>'`, or Langfuse tables with
`clickhouse/discover.py --env-file PATH --url URL`. Output is human-readable;
add `--json` for automation. Discovery is limited by credential visibility:
an empty result does not prove an object does not exist.

Replace `<profile_name>` with your configured connection name. Available profiles
can be listed with the vendor CLIs: `snow connection list` or
`databricks auth profiles` (the latter also makes API calls to validate profiles).
ClickHouse uses `CLICKHOUSE_*` settings from an env file or the environment instead
of profiles; see its [setup](clickhouse/README.md#setup). Logfire uses a project read
token in `LOGFIRE_READ_TOKEN`; see its [setup](pydantic-logfire/README.md#setup).

- [Discovery and agent usage](docs/agent-usage.md)
- [JSONL schema, validation, and formatter reader](docs/output-format.md)

Shared discovery/contract tests: `python -m unittest discover -s tests -v`.
