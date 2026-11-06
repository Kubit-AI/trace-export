# Pydantic Logfire export

Exports spans from a Logfire project's `records` table as GenAI OTLP JSONL, using
the Logfire query API. Each line contains one span from one row.

## Setup

Requires Python 3.10+. From the repository root:

```sh
python3 -m venv .venv  # skip if it already exists
source .venv/bin/activate
python -m pip install -r pydantic-logfire/requirements.txt
```

Create a **read token** in Logfire (project settings → Read tokens). A read token
is scoped to one project. Supply it as `LOGFIRE_READ_TOKEN`, either in the process
environment or in an owner-only env file:

```sh
cp pydantic-logfire/.env.example pydantic-logfire/.env
chmod 600 pydantic-logfire/.env
```

`pydantic-logfire/.env` is read when present; `--env-file PATH` reads another
file. The process environment overrides the file. The file is parsed, never
executed. Never put the token on the command line or in source files. Error
messages redact tokens.

The API region (US or EU) is inferred from the token. Set `LOGFIRE_BASE_URL` or
`--base-url` for self-hosted Logfire.

## Export

```sh
python pydantic-logfire/export_traces.py --limit 5
python pydantic-logfire/export_traces.py --no-sample \
  --since 2026-10-01T00:00:00Z --until 2026-10-30T00:00:00Z \
  --output data/logfire/october.otel.jsonl \
  --keep-raw data/logfire/october.raw.jsonl --json-summary
```

Exports default to **up to 10 span rows from the last 24 hours**, printed to
stdout. For a file export, use `--no-sample` with both time bounds or
`--all-time`. See [sampling](../docs/sampling.md). The first line of stderr
names the Logfire organization and project the token reads.

- `--since`/`--until`: filter span `start_timestamp` (inclusive/exclusive).
- `--limit N`: span rows, not traces.
- `--environment NAME`: only rows from that deployment environment; repeatable.
- `--page-size N`: rows per API request, up to 10,000 (default 1,000).
- `--max-retries N`: consecutive failed attempts per request (default 8).
- `--query-timeout SECONDS`: per-request HTTP timeout (default 300).
- `--format raw`: write rows as returned by the API, every column.
- `--keep-raw PATH`, `--overwrite`, `--json-summary`: as for the other exporters.
- `--from-raw PATH --output PATH`: convert a saved raw export without API access.

### Reliability

- **No row cap.** Rows are fetched in `(start_timestamp, trace_id, span_id)`
  order, and each request continues after the last row received. Unlike fixed
  time windows, a busy period cannot exceed a per-request cap and lose rows, and
  boundary rows are never fetched twice.
- **Retries.** Rate limits (429) wait for the server's `Retry-After`. Timeouts,
  connection errors and 5xx responses retry with exponential backoff; timeouts
  and 5xx also halve the page size, which grows back after successes. Invalid
  tokens and SQL errors fail immediately.
- **Resume.** With `--keep-raw PATH` (or `--format raw --output PATH`), each page
  is saved to disk as it arrives. If the export stops, rerun the same command
  with `--resume`: a partial last line is removed and the download continues
  after the last saved row. `--since`, `--until` and `--environment` must match;
  a `PATH.state.json` file records them until the download completes.

**Use an `--until` a few minutes in the past.** Logfire stores a span when it
ends, so a long span that started before `--until` but has not finished yet is
missing from the export. Recent spans can also arrive with a small delay.

**Exports can contain partial traces.** The time window filters individual
spans, and the limit can split a trace. Missing parents are expected.

**History is limited by your Logfire plan's retention.** Older data cannot be
exported.

## Mapping

Logfire stores OpenTelemetry spans almost verbatim, so the conversion is direct:

| OTLP field | Logfire column |
| --- | --- |
| trace/span/parent IDs, name | `trace_id`, `span_id`, `parent_span_id`, `span_name` |
| start/end time | `start_timestamp`, `end_timestamp` (microsecond precision) |
| span attributes | `attributes` |
| events, links | `otel_events`, `otel_links` |
| status | `otel_status_code` (`UNSET`/`OK`/`ERROR`), `otel_status_message` |
| scope | `otel_scope_name`, `otel_scope_version`, `otel_scope_attributes` |
| resource attributes | `otel_resource_attributes`, plus `logfire.project.id` |

- SDK attributes that Logfire moves into columns are restored on the span:
  `logfire.span_type` (`span` or `log`), `logfire.msg`, `logfire.level_num`,
  `logfire.tags` and `logfire.json_schema`.
- Logfire logs (`kind = log`) are zero-duration spans, as the SDK sends them.
- Pending-span placeholders are skipped.
- **The OTel span kind is not stored by Logfire** and is exported as
  `UNSPECIFIED` (0).
- Error spans get `error.type` from the exception type when absent.
- Columns derived from other fields (`service_*`, `http_*`, `url_*`,
  `exception_*`, `duration`, `created_at`, `day`) are not repeated. Use
  `--format raw` or `--keep-raw` to retain them.

### GenAI

Pydantic AI already emits GenAI semantic-convention attributes
(`gen_ai.operation.name`, `gen_ai.input.messages`, `gen_ai.output.messages`,
`gen_ai.usage.*`, `gen_ai.tool.*`, ...); they are kept as stored. One
normalization: Pydantic AI writes tool results in `tool_call_response` message
parts as `result`; v1.39.0 names it `response`, so it is renamed.

**Prompts and responses are present only if the application captured them.**
With `logfire.instrument_pydantic_ai(include_content=False)`, message parts have
no `content`, tool arguments and results are absent, and the converter warns
`gen_ai_messages_not_schema_compatible`. Logfire's scrubbing also replaces
values that look sensitive (for example `session_id`) at ingestion.

## Tests

```sh
python -m unittest discover -s pydantic-logfire/tests -v
```

Tests use synthetic rows and a fake query client; they never contact Logfire.
