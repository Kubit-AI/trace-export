# ClickHouse Langfuse export

Exports spans from a self-hosted Langfuse v4 ClickHouse `events_full` table as
GenAI OTLP JSONL. Each line contains one span from one table row. The exporter
reads ClickHouse directly over its HTTP interface; it does not use the Langfuse API.

## Setup

Requires Python 3.10+. From the repository root:

```sh
python3 -m venv .venv  # skip if it already exists
source .venv/bin/activate
python -m pip install -r clickhouse/requirements.txt
cp clickhouse/.env.example clickhouse/.env
chmod 600 clickhouse/.env
```

Edit `clickhouse/.env` to set these keys:

| Key | Meaning |
| --- | --- |
| `CLICKHOUSE_URL` | `http://host:port` or `https://host:port`; required |
| `CLICKHOUSE_USER` | ClickHouse user; required |
| `CLICKHOUSE_PASSWORD` | Password; empty when unset |
| `CLICKHOUSE_DB` | Session and export database (default `default`) |
| `CLICKHOUSE_CA_CERT` | Optional CA certificate file for `https` URLs |

They are read from `clickhouse/.env` when it exists, or from `--env-file PATH`.
Process environment variables override the file; `--url` and `--database`
override both. The file is parsed, never executed as shell code, and `${...}`
references are not interpolated. Other keys in the file are ignored. An existing
env file that already defines `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`, such as
a self-hosted Langfuse deployment's, can be reused with `--env-file` and `--url`,
without copying its values:

```sh
python clickhouse/discover.py --env-file /path/to/config.env \
  --url http://127.0.0.1:8123
```

Never paste credentials into commands, documentation, or prompts. Put them in an
owner-only file (`chmod 600`) or in process environment variables supplied by a
secret manager. The root `.gitignore` excludes `.env` files in every directory.
URLs containing `user:password@` are rejected; credentials belong in the dedicated
keys. Error messages redact URL credentials, the password, and its HTTP basic
authentication form. Do not rely on redaction as the only protection.

The URL must not contain a path, query, or fragment. Its port defaults to 80/443,
not ClickHouse's usual HTTP ports, so include `:8123` or `:8443` when needed.
The native protocol (port 9000) is not supported. **Plain `http://` sends the
password unencrypted**: use it only for a local or otherwise trusted network. With
`https://`, certificate verification is always on; there is no option to disable
it. Use `CLICKHOUSE_CA_CERT` for a private certificate authority.

The user needs `SELECT` on `<database>.events_full` and metadata visibility of
`system.tables`/`system.columns`. Before each data query, the exporter runs a
constant preflight query to prove the server accepts its output and timeout
settings. A `readonly=1` user, or a settings profile with conflicting constraints,
fails there without querying trace data; `readonly=2` allows these session settings.

## Discover

```sh
python clickhouse/discover.py
python clickhouse/discover.py --database default --json
```

Discovery lists tables named exactly `events_full` in every database visible to
the user (`system.tables` shows only tables on which the user has a grant).
`--database` limits the search to one database; `CLICKHOUSE_DB` only selects the
session database and does not filter. `--match` is a case-insensitive glob on the
**database** name. Each match is then checked against the supported Langfuse v4
engine and column types, using metadata only:

| Validation | Meaning |
| --- | --- |
| `schema_supported` | Engine and columns are supported; SELECT access not checked |
| `unsupported_schema` | Not a supported events table; `schema_problem` explains why |
| `unverified` | The schema check failed, for example due to permissions |
| `candidate` | Name matches, but the database name is unsupported by the exporter |

`export_args` contains `--database` and `--table`; append your own `--env-file`
and `--url` when not using `clickhouse/.env`, plus time, project, and output options. **Discovery does not list projects
and never selects a project implicitly.** It reads no rows, so it cannot tell
whether a table contains traces. Result fields and exit codes are shared with the
other sources; see [discovery usage](../docs/agent-usage.md).

## Export

```sh
python clickhouse/export_traces.py \
  --since 2026-10-03T00:00:00Z --until 2026-10-04T00:00:00Z --limit 5
python clickhouse/export_traces.py \
  --project-id '<project_id>' --no-sample \
  --since 2026-10-03T00:00:00Z --until 2026-10-04T00:00:00Z \
  --output data/clickhouse/day.otel.jsonl --json-summary
```

Exports default to **up to 10 span rows from the last 24 hours**, unsorted, with
a 5-minute server-side timeout (`max_execution_time`), printed to stdout. For a
file export, use `--no-sample` with both time bounds or `--all-time`. See
[sampling and query limits](../docs/sampling.md).

- `--project-id ID`: export one Langfuse project. **Without it, every project
  readable by the user is exported** in one query, and `--limit` is global across
  projects. The project is never read from Langfuse initialization settings.
- `--since`/`--until`: filter span `start_time` (inclusive/exclusive). Timezone required.
- `--limit N`: span rows, not traces.
- `--database`, `--table`: default `CLICKHOUSE_DB` (or `default`) and `events_full`.
- `--batch-size N`: server `max_block_size` hint; rows still stream from one query.
- `--format raw`: write source rows as returned by ClickHouse, all observation types.
- `--keep-raw PATH`, `--overwrite`, `--json-summary`, `--order-by-time`: as for the other exporters.

**Exports can contain partial traces.** The time window filters individual spans,
so a trace's spans that started outside it are omitted; the limit can also split
a trace. Missing parents are expected in such files.

The query reads `events_full FINAL`, so only the latest `event_ts` version of each
span is exported, then excludes rows whose latest version has `is_deleted = 1`.
`FINAL` merges at read time and can cost more than a plain scan; narrow the window
and project when possible. The table schema is checked first: the engine must be
`ReplacingMergeTree(event_ts, is_deleted)` (replicated variants included), and
its sorting key must identify one observation: `project_id`, `trace_id` (or
Langfuse's `xxHash32(trace_id)`), `span_id`, and `start_time`, optionally with
`toStartOfMinute(start_time)`. Any other key expression, such as `event_ts` or
`is_deleted`, is rejected because `FINAL` would then keep superseded or deleted
versions.

Columns are selected by name. `ingestion_api_key` (a hashed ingestion credential)
is never selected, and is dropped during conversion even when an external raw file
contains it. Optional enrichment columns that are absent are listed on stderr.
Raw rows are written exactly as the server returns them: 64-bit integers stay
unquoted and decimals are quoted strings, so neither passes through Python floats.
The server replaces invalid UTF-8 in string values with U+FFFD; the original bytes
are not recoverable from either format. Mid-stream server errors and truncated rows
fail the export without writing output or echoing row content.

## Convert a saved raw export

No ClickHouse driver or credentials are needed:

```sh
python clickhouse/convert_to_otel.py \
  --input data/clickhouse/day.raw.jsonl --output data/clickhouse/day.otel.jsonl
```

By default the first invalid row fails conversion and leaves existing output
untouched; `--skip-invalid` skips whole rows with line-number diagnostics.

## Mapping

Targets GenAI semantic conventions v1.39.0. The converter is
`trace_export/langfuse.py`; message normalization is shared with the other sources.

| Langfuse type | GenAI mapping |
| --- | --- |
| `AGENT` | `invoke_agent`, agent name from the observation name |
| `GENERATION` | `chat`, `provided_model_name`, effective input/output usage |
| `TOOL` | `execute_tool`, tool name, input/output as arguments/result |
| Other types | Ordinary spans with the original name |

**IDs and times are used as stored; nothing is remapped.** Trace IDs must be 32
and span/parent IDs 16 hexadecimal characters (no suffixes). An empty or all-zero
parent marks a root. A row with any other ID shape, or without `end_time` (for
example an unfinished observation), fails conversion; use `--format raw` for such
data. Reversed times become flagged zero-duration spans as for the other sources.
Times are `DateTime64(6)` microseconds expressed as nanoseconds; finer precision
from the original instrumentation is not available in the table.

The exporter performs no joins or enrichment: no traces table, scores, project
names, or model prices are queried. Each resource carries `langfuse.project.id`.
Trace IDs are only unique within a project, so **group spans by project ID and
trace ID** when a file contains several projects.

Langfuse flattens span metadata into `metadata_names`/`metadata_values`.
`attributes.<key>` entries are restored as span attributes and
`resourceAttributes.<key>` entries as resource attributes. **Langfuse already
stringified these values**, so they stay strings (`"3"`, not `3`); original types
are not guessed. Restored `gen_ai.*` attributes take precedence over mapped values.
Two restored kinds are normalized: message lists that satisfy the pinned message
schemas become structured values, and digit-only token counts within int64 become
integers. Other restored values stay verbatim strings and produce an
`existing_attribute_not_schema_compatible:<key>` warning.

Mapped messages come from `input`/`output` when no restored message attribute
exists; unsupported shapes stay in the source row with a `payload_unmapped`
warning. No provider is inferred; usage is the observation's own `input`/`output`
count, without totals or cache additions; tool call IDs are never invented. Level
`ERROR` sets error status, with `status_message` as its message, and
`error.type=_OTHER` on mapped spans. The OTel span kind is not stored by Langfuse
and is exported as unspecified (0). The scope comes from `scope_name`/`scope_version`.

The complete source row (except `ingestion_api_key`) is preserved in the
`trace_export.langfuse.row` span attribute. Embedded Langfuse fields such as user,
tags, prompt, cost, release, version, experiment, completion start time, and other
metadata entries are available only there. UInt64 values outside the int64 range
are kept as exact decimal strings in that attribute, and decimals remain strings.
Integers outside int64 inside mapped tool arguments/results and messages are
likewise exported as exact decimal strings.
The table has no columns for the original OTel span events or links, so exported
spans have none; Langfuse `EVENT` observations are exported as ordinary spans.

See the [shared output contract](../docs/output-format.md) and the synthetic
[ClickHouse example](../examples/clickhouse.otel.jsonl).

## Verification

```sh
python -m pip install -r snowflake/requirements-test.txt
python -m unittest discover -s clickhouse/tests -v
```

Tests use synthetic rows and a schema-only `events_full` fixture; they need no server
or credentials. The synthetic example can be checked against the output contract
offline:

```sh
python -m pip install -r requirements-validation.txt
python -m trace_export.validate examples/clickhouse.otel.jsonl --json
```

An actual ClickHouse server is required to validate permissions, server settings,
and the live query end to end. Downstream collector/backend import is not yet tested.
