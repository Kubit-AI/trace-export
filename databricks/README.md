# Databricks MLflow trace export

Exports a `<agent_name>_trace_unified` view as GenAI OTLP JSONL. Each line contains
all spans from one database row, grouped by original resource and instrumentation
scope. No span explosion is performed in SQL.

## Setup

Requires Python 3.10+. From the repository root:

```sh
python3 -m venv .venv  # skip if it already exists
source .venv/bin/activate
python -m pip install -r databricks/requirements.txt
```

From inside `databricks/`, use `../.venv` and `-r requirements.txt` instead.
Use a named connection profile in `~/.databrickscfg` (INI format):

```ini
[profile_name]
host = https://your-workspace.cloud.databricks.com
token = your-personal-access-token
warehouse_id = your-warehouse-id
```

Choose your own section name and replace `<profile_name>` in commands with it.
If the Databricks CLI is installed, `databricks auth profiles` lists profiles and
checks them using API requests. The exporter does not parse profile files itself.

Use the warehouse ID from the SQL warehouse's connection details. Restrict file
access with `chmod 600 ~/.databrickscfg`. Existing Databricks CLI/SDK profiles also
work: authentication is delegated to the SDK, including supported OAuth profiles
and credential refresh. For OAuth machine-to-machine authentication, replace
`token` with `client_id` and `client_secret`.

Pass `--connection '<profile_name>'` to select this profile, or `--connection DEFAULT` for
the default profile. `DATABRICKS_CONFIG_FILE` selects an alternative profile file.
`--http-path /sql/1.0/warehouses/ID` overrides the profile's warehouse selection.
The SDK's standard precedence applies: process environment variables override
profile fields. With `--connection`, the exporter does **not** read `.env` or its
catalog/schema defaults. Set catalog/schema on the command line or use a fully
qualified table/agent name.

The user needs warehouse access, catalog/schema usage and SELECT on the view.

### Optional development `.env`

Without `--connection`, existing environment-based commands still work. The script
automatically loads `databricks/.env`, regardless of working directory. See
`.env.example`, copied with `cp databricks/.env.example databricks/.env`:

```dotenv
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=replace-with-your-token
DATABRICKS_CATALOG=my_catalog
DATABRICKS_SCHEMA=my_schema
```

Process environment variables override `.env`; the file is parsed, never executed
as shell code. `--env-file PATH` selects another file and cannot be combined with
`--connection`. The root `.gitignore` excludes `.env` files in every directory.

## Export

Exports now default to **up to 10 matching rows from the last 24 hours**, without
sorting and with a 5-minute server-side query timeout. Omit `--output` to print
sample JSONL to stdout; diagnostics stay on stderr. Explicit `--limit` and time
bounds override the defaults. For a fuller file export, use `--no-sample` with both
time bounds or `--all-time`. See [sampling and query limits](../docs/sampling.md).

To find tables first, run `python databricks/discover.py --connection '<profile_name>'`.
Use `--catalog my_catalog --schema my_schema` to narrow the search, and `--json`
for machine-readable output. The default is human-readable. Discovery checks
matching tables' required columns without reading trace payloads; an empty result
can reflect permissions. See [discovery usage](../docs/agent-usage.md).

From the repository root:

```sh
python databricks/export_traces.py \
  --connection '<profile_name>' \
  --table my_catalog.my_schema.my_agent_trace_unified \
  --output data/databricks/my_agent.otel.jsonl
```

From inside `databricks/`:

```sh
./export_traces.py --connection '<profile_name>' \
  --agent my_catalog.my_schema.my_agent \
  --output ../data/databricks/my_agent.otel.jsonl
```

`--agent` appends `_trace_unified` unless already present, so
`--agent my_catalog.my_schema.my_agent_trace_unified` also works.
`--table` uses the exact view name. `--catalog`/`--schema` override environment
defaults for unqualified names; fully qualified names specify all three parts.
Names must start with a letter/underscore and contain letters, digits/underscores.

- `--list-agents`: list unified views in the configured catalog/schema.
- `--since 2026-09-01T00:00:00Z --until 2026-10-01T00:00:00Z`: filter
  `request_time`, inclusive lower/exclusive upper bound. Timezone required.
- `--limit 20`: limit database rows (whole traces), not individual spans.
- `--format raw`: export original database rows as JSONL without conversion.
- `--keep-raw data/databricks/raw.jsonl`: explicitly retain the raw download.
- `--overwrite`: atomically replace output only after success.
- `--json-summary`: emit a machine-readable export result on stdout.

The [shared output contract](../docs/output-format.md) provides a versioned JSON
Schema, local validator, synthetic examples, and a Python reader for formatters.

Raw downloads are temporary and deleted after conversion, including on ordinary
failure. Forced termination can leave temporary files behind. Downloads use
`fetchmany` (default 1,000 rows) and server-side JSON with nulls retained. Raw
numbers are not round-tripped through Python floats. Output files use owner-only
permissions. No append, checkpoints or deduplication. The connector uses a
60-second socket timeout and two retry attempts; a running warehouse query or
warehouse startup can take longer. Dependencies include the connector's PyArrow
extra, enabling Arrow/CloudFetch for larger downloads while retaining the standard
row API. This also removes the missing-PyArrow warning. Actual download speed
depends on result size and the warehouse; local conversion is unchanged.

All trace states are queried, including failures, with no implicit client request
ID filter. Unfinished traces with missing span end timestamps cannot be converted
yet; use raw export for those. Empty spans arrays remain empty trace envelopes.
Malformed data fails conversion instead of silently dropping individual spans.

## Convert a saved raw export

No connector, credentials or extra runtime dependencies are needed:

```sh
python databricks/convert_to_otel.py \
  --input data/databricks/raw.jsonl --output data/databricks/converted.jsonl
```

Input stays untouched. Explicit `--skip-invalid` skips an entire bad database row;
the default fails with a row/span location and leaves existing output untouched.

## Mapping

Targets GenAI semantic conventions v1.39.0, shared with Snowflake. Shared message
normalization is in `trace_export/genai.py`; Databricks mappings are in
`trace_export/mlflow.py`.

| MLflow span type/field | GenAI mapping |
| --- | --- |
| `AGENT` | `invoke_agent`, agent name |
| `CHAT_MODEL`, `LLM` | `chat`, model/provider, per-span token counts |
| `TOOL` | `execute_tool`, tool name, arguments/result, call ID when supplied |
| Other types | Ordinary OTel spans, original names |
| `mlflow.trace.session` | Conversation ID on classified spans |
| `mlflow.spanInputs`, `mlflow.spanOutputs` | Messages on agent/model/root spans |

Existing `gen_ai.*` values take precedence. Provider is only mapped when supplied;
Databricks storage alone does not identify a model provider. Trace-level token
aggregates stay in metadata rather than being copied to every span. MLflow `tr-`
and `trace:/location/` prefixes are removed to recover the original hexadecimal
OTel trace ID. Arbitrary IDs are not fabricated.

Supports `messages`, agent `input`/`output` wrappers, OpenAI-style `choices`,
tool-call-only responses, and `text`/`input_text`/`output_text` content. Missing
finish reasons become `unknown` with summary notes. Unsupported payloads remain
in source attributes with warnings.

Original span attributes remain available. Other span fields are retained under
`trace_export.databricks.span`; other row columns (including tags, metadata,
assessments, request/response and client request ID) are retained under
`trace_export.databricks.row` in scope attributes. Original resources/scopes,
schema URLs, statuses, flags, dropped counts, embedded events and links map to
OTLP. This does not query separate span-event tables.

Reversed timestamps become zero-duration spans, flagged with
`trace_export.timing.corrected=true` and reason `end_precedes_start`; originals
remain available. Their actual duration is unknown. IDs and parent links are
preserved. Nanosecond integers remain exact; other numbers use OTLP int64/double.

## Verification

```sh
python -m pip install -r snowflake/requirements-test.txt
python -m unittest discover -s databricks/tests -v
python -m unittest discover -s snowflake/tests -v
```

Downstream collector/backend import is not yet tested.

References: [connection profiles](https://docs.databricks.com/aws/en/dev-tools/auth/config-profiles),
[SDK authentication](https://databricks-sdk-py.readthedocs.io/en/stable/authentication.html),
[SQL connector](https://docs.databricks.com/aws/en/dev-tools/python-sql-connector),
[unified views](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/observe-with-traces/query-via-sdk),
[MLflow trace IDs](https://mlflow.org/docs/latest/api_reference/_modules/mlflow/tracing/utils.html).
