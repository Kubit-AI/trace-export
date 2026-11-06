# Sampling and query limits

All exporters default to `--sample`: **up to 10 matching database rows**, an
implicit last-24-hours window, no sorting, and a 300-second server-side statement
timeout. `--limit N` overrides the sample size; `--output PATH` writes a file.
Without `--output`, sample JSONL goes to stdout:

```sh
python snowflake/export_traces.py --connection '<profile_name>' \
  --agent MY_DB.MY_SCHEMA.MY_AGENT
python databricks/export_traces.py --connection '<profile_name>' \
  --table my_catalog.my_schema.my_agent_trace_unified --limit 5
python clickhouse/export_traces.py --project-id '<project_id>'
```

`--sample` can be supplied explicitly but is not necessary. A sample is not
random, representative, or necessarily the newest data. Snowflake limits span
rows and can split traces; Databricks limits whole database trace rows, each of
which may contain many spans and large payloads. ClickHouse limits span rows like
Snowflake; without `--project-id` it queries every readable Langfuse project and
the limit is global across them. Ten rows is not a byte/token cap.

## Time bounds

Sample mode resolves bounds once at command start:

| Supplied options | Effective window |
| --- | --- |
| Neither | Now minus 24 hours, through now |
| Only `--until` | Preceding 24 hours, ending at that timestamp |
| Only `--since` | That timestamp through now |
| Both | Exactly the supplied bounds |
| `--all-time` | All history, still subject to the sample row cap |

Bounds are inclusive `--since` and exclusive `--until`. Empty results never cause
an automatic retry with a wider range. `--all-time` conflicts with time bounds.
ClickHouse bounds apply to each span's `start_time`, so a window can include only
part of a trace.

## Fuller exports

`--no-sample` removes the implicit row cap. It requires an output file and **both**
time bounds, or explicit `--all-time`. An explicit `--limit` still applies:

```sh
python databricks/export_traces.py --connection '<profile_name>' \
  --table my_catalog.my_schema.my_agent_trace_unified \
  --no-sample --since 2026-09-01T00:00:00Z --until 2026-09-02T00:00:00Z \
  --output data/day.jsonl --json-summary
```

To restore the old full-history behavior, use `--no-sample --all-time --output PATH`.
The timeout remains enabled. Neither mode sorts by default; `--order-by-time`
explicitly enables ascending timestamp order and can add query cost.

## Output and completion

Progress, the resolved query bounds, and warnings go to stderr. Sample stdout is
only JSONL; raw mode works there too. Downloads and conversion finish in temporary
files before any stdout data is emitted. Conversion failure therefore emits no
trace data, though interruption or a broken output pipe during emission can still
leave partial stdout. Temporary files are deleted on normal completion/failure.
`--keep-raw PATH` retains the raw download explicitly, including for stdout samples.

`--json-summary` requires `--output` to avoid mixing a summary with trace records.
Its result includes `sample`, `limit`, `since`, `until`, `query_timeout_seconds`,
and `possibly_truncated`. Reaching the row cap is reported as possibly truncated,
not proof that more rows exist. No additional count query is issued. A completed
sample is not a guarantee of complete traces.

## Cost controls

`--query-timeout SECONDS` overrides the default 300 seconds (allowed range
1–172800). Before the data query, each exporter sets a timeout in its own session;
if this fails, it does not run the export query. This uses
[Snowflake STATEMENT_TIMEOUT_IN_SECONDS](https://docs.snowflake.com/en/sql-reference/parameters#statement-timeout-in-seconds),
[Databricks STATEMENT_TIMEOUT](https://docs.databricks.com/aws/en/sql/language-manual/parameters/statement_timeout),
and ClickHouse [max_execution_time](https://clickhouse.com/docs/operations/settings/settings#max_execution_time)
as a per-query setting, verified by a constant preflight query.
It does not alter persistent warehouse settings. Session settings can override
inherited defaults according to the platform's precedence rules.

This is a per-statement server timeout, not an end-to-end process deadline:
connection setup, warehouse startup, fetching, and local conversion have different
timing behavior. The limit/time window/timeout reduce accidental large exports;
they do not guarantee a monetary ceiling. A database may scan substantial data
before finding matching rows even with `LIMIT`; ClickHouse also reads
`events_full FINAL` to deduplicate span versions, which merges at read time.
Discovery remains metadata-only and does not adopt the export sampling/time-window defaults.
