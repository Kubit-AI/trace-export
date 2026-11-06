# Snowflake Cortex Agent export

To find agents, run `python snowflake/discover.py --connection '<profile_name>'` from the
repository root. Narrow the search with `--database NAME --schema NAME` and use
`--json` for machine-readable output; human-readable output is the default.
An empty result does not establish that an agent is absent: permissions can hide
it. See [discovery usage](../docs/agent-usage.md) for visibility and partial results.

Exports accept `--json-summary` for machine-readable completion details. The
[shared output contract](../docs/output-format.md) includes a JSON Schema, local
validator, synthetic examples, and a Python reader for other formatters.

Requires Python 3.10+ and a Snowflake connection with access to the agent.
Replace `<profile_name>` in commands with your configured connection name.
If the Snowflake CLI is installed, `snow connection list` lists its connections.
We recommend a virtual environment to keep the connector and its dependencies
isolated from your system Python. Create it once, then activate it in each new
terminal before running the exporter.

From the repository root:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r snowflake/requirements.txt
```

If your terminal is already inside `snowflake/`, use these commands instead
(they use the same environment):

```sh
python3 -m venv ../.venv
source ../.venv/bin/activate
python -m pip install -r requirements.txt
```

Skip the creation step if `.venv` already exists. Run `deactivate` when finished.
The remaining examples assume you are at the repository root.

Create a profile in your Python connector's `connections.toml`, for example
`~/.snowflake/connections.toml` when `~/.snowflake` exists:

```toml
[profile_name]
account = "myorg-myaccount"
user = "user@example.com"
authenticator = "externalbrowser"
warehouse = "COMPUTE_WH"
role = "AGENT_MONITOR_ROLE"
```

Use your actual account, user, warehouse and role. This example uses browser SSO;
existing connector profiles using other authentication methods work too. See
[connector profile configuration](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect).

```sh
python snowflake/export_traces.py \
  --connection '<profile_name>' \
  --database MY_DB --schema MY_SCHEMA --agent MY_AGENT \
  --no-sample --since 2026-09-24T00:00:00Z --until 2026-09-25T00:00:00Z \
  --output data/snowflake/MY_AGENT-2026-09-24.jsonl
```

See [sampling and query limits](../docs/sampling.md) for the default 10-row cap,
stdout output, time-window rules, and 5-minute server timeout. Explicit `--limit`
and time bounds are honored; omit `--output` to print a sample as JSONL.

The default output is **GenAI OTLP JSONL**, one span inside an OTLP request
envelope per line. The raw download is temporary and deleted after conversion,
including on ordinary failure. To explicitly retain the raw download, add
`--keep-raw data/snowflake/MY_AGENT.raw.jsonl`. To export only raw database rows,
use `--format raw`.

You can supply a fully qualified agent name instead of separate database/schema
options:

```sh
python snowflake/export_traces.py --connection '<profile_name>' \
  --agent MY_DB.MY_SCHEMA.MY_AGENT \
  --output data/snowflake/MY_AGENT.otel.jsonl
```

Qualified names use three dot-separated exact stored names without SQL quotes.
For names containing literal dots, use the separate options. When both
`--database` and `--schema` are supplied, `--agent` is always a literal name.
Supplying only one of those two options is an error.

The default is a sample of up to 10 rows from the last 24 hours.
Use `--no-sample --all-time --output PATH` for all available spans. Omit `--connection` to use
the connector's default profile. Names are passed as values: use exact stored
names (usually uppercase for objects created without quotes), without SQL quotes.
Use `--limit 100` for a sample and `--help` for all options.
By default only `RECORD_TYPE = 'SPAN'` rows are exported, matching the Cortex span
source filter, and time bounds use `START_TIMESTAMP`. To include logs, feedback,
and other event types in a raw export, use
`--format raw --all-events --time-column TIMESTAMP`. OTLP conversion skips
non-span records and reports their count; `SPAN_EVENT` attachment is not supported.

The exporter uses the documented
[`SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS`](https://docs.snowflake.com/en/sql-reference/functions/get_ai_observability_events-snowflake-local)
with object type `CORTEX AGENT`.

## Permissions and unredacted traces

Discovery and trace access are separate. `SHOW AGENTS` returns agents visible to
the current role; an empty result can mean absent objects, filters, or missing
privileges. Seeing an agent does not prove that the role can read its traces.

For `GET_AI_OBSERVABILITY_EVENTS`, the querying role needs the
`SNOWFLAKE.CORTEX_USER` database role and `MONITOR` (or `OWNERSHIP`) on the agent,
plus access to the parent database/schema and the query warehouse.

An administrator with the appropriate grant authority can provision access as
follows. Replace the example role, database, schema, agent, and warehouse names:

```sql
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE YOUR_EXPORT_ROLE;
GRANT USAGE ON DATABASE MY_DB TO ROLE YOUR_EXPORT_ROLE;
GRANT USAGE ON SCHEMA MY_DB.MY_SCHEMA TO ROLE YOUR_EXPORT_ROLE;
GRANT MONITOR ON AGENT MY_DB.MY_SCHEMA.MY_AGENT TO ROLE YOUR_EXPORT_ROLE;
GRANT USAGE ON WAREHOUSE MY_WAREHOUSE TO ROLE YOUR_EXPORT_ROLE;
```

For full conversation text, tool inputs/outputs, and user feedback, an account
administrator must additionally grant the account-level privilege:

```sql
USE ROLE ACCOUNTADMIN;
GRANT READ UNREDACTED AI OBSERVABILITY EVENTS TABLE
    ON ACCOUNT TO ROLE YOUR_EXPORT_ROLE;
```

This extra grant controls payload redaction; it does not replace agent access or
make hidden agents discoverable. Without it, an otherwise authorized role can
still receive metadata such as token usage and latency. Missing content is not
proof of a missing grant, and the exporter cannot reconstruct redacted payloads.
Use the configured export role for downloads; `ACCOUNTADMIN` above is only for
the administrator's grant operation. The scripts never issue grants themselves.

Exports warn once if at least one raw row was returned but all recognized
conversation content, tool inputs/outputs, and user-feedback fields are empty or
absent across those rows. The warning names the unredacted privilege and explicitly
allows for genuinely empty content or a metadata-only sample. It is a heuristic,
not a privilege check or a diagnosis of redaction. Any nonempty recognized payload
suppresses it; partially redacted exports may therefore go undetected. Unknown
payload fields may also cause a false positive. The check reads the local raw
download, issues no extra database queries, and does not alter data or fail export.
It applies to both raw and OTLP exports, including stdout samples. The warning goes
to stderr and to the `warnings` array in `--json-summary`, with code
`snowflake_payloads_empty_possible_redaction`. Zero-row exports do not trigger it.

References: [SHOW AGENTS visibility](https://docs.snowflake.com/en/sql-reference/sql/show-agents),
[Cortex monitoring access](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-monitor),
[unredacted observability privilege](https://docs.snowflake.com/en/release-notes/bcr-bundles/un-bundled/bcr-read-unredacted-ai-observability-events).

## Output behavior


- The raw download includes all returned columns. Each row is serialized by
  Snowflake using `TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(e.*))`: SQL null columns are
  retained, objects/arrays stay nested, and numbers are not converted through
  Python floating point. Timestamps use Snowflake's JSON serialization with the
  session timezone set to UTC. Column names retain their returned case.
- Rows are fetched in batches (default 1,000). No `fetchall` or OFFSET pagination.
- Time bounds apply to `START_TIMESTAMP` by default: inclusive `--since`, exclusive
  `--until`. Explicit timezone offsets are required. Rows are unsorted by default; `--order-by-time` opts into timestamp order.
  Equal timestamps have no guaranteed relative order. `--time-column TIMESTAMP`
  instead uses span end/event time. Rows with a null selected timestamp are
  excluded when a time bound is supplied; without bounds they are retained.
- Each run creates a complete new file, including an empty file for zero rows.
  Existing files are refused unless `--overwrite` is passed. A temporary file in
  the same directory is published atomically only after successful fetching and
  writing. Ordinary errors clean it up; a forced process termination can leave
  a hidden `.partial` file. Files are created with owner-only permissions.
- No append, checkpoint, or deduplication yet. Limits/time windows can split a
  trace. Late-arriving telemetry may require re-exporting a previous window.

Offline checks (test validators required, no credentials needed):

```sh
python -m pip install -r snowflake/requirements-test.txt
python -m unittest discover -s snowflake/tests -v
```

An actual account is required to validate permissions, function availability,
and the live query end to end.

## Source field mapping

The raw fields needed for Cortex Agent trace analysis are retained:

| Raw column | Used for |
| --- | --- |
| `START_TIMESTAMP`, `TIMESTAMP` | Start/end, duration, invalid timing |
| `TRACE` | Trace ID and span ID |
| `RECORD` | Parent span ID, span name/kind, status and error |
| `RECORD_ATTRIBUTES` | Agent/model, prompts/responses, planning and conversation messages, tool IDs/names/arguments/status, thread/user IDs, input/output/cache tokens |
| `RESOURCE_ATTRIBUTES` | User identity fallback |
| `VALUE`, `RECORD_TYPE` | Raw payload and record classification |

Raw JSONL contains the original column names and payloads, without derived
aliases. All span kinds are kept, including root Agent, planning, and
tool spans. No valid-ID, action-only, or successful-span filter is applied.
Only the source span filter is applied, not downstream report filters.
This exporter does not assert trace completeness, apply conversation windows or
settling periods, or evaluate agent quality.

## Convert an existing raw file

The converter needs only Python 3.10+ (no connector, credentials, or OTel SDK):

```sh
python snowflake/convert_to_otel.py \
  --input data/snowflake/MY_AGENT.raw.jsonl \
  --output data/snowflake/MY_AGENT.otel.jsonl
```

From inside `snowflake/`, use `python convert_to_otel.py` instead.
The standalone converter leaves its input untouched. The export command deletes
only the temporary raw file it created itself. A forced process termination can
leave temporary files behind.

## GenAI mapping contract

Mapping version `1.2.0` targets the experimental OpenTelemetry semantic conventions
**v1.39.0**, deliberately pinned rather than following upstream changes automatically.
The mapping version is the instrumentation scope `version`; the semantic conventions
version is the `trace_export.gen_ai.semconv_version` scope attribute. References:
[GenAI attributes](https://github.com/open-telemetry/semantic-conventions/blob/v1.39.0/docs/registry/attributes/gen-ai.md),
[agent/tool spans](https://github.com/open-telemetry/semantic-conventions/blob/v1.39.0/docs/gen-ai/gen-ai-agent-spans.md),
[OTLP file format](https://opentelemetry.io/docs/specs/otel/protocol/file-exporter/).

| Source | Mapping |
| --- | --- |
| Span named `Agent` | `invoke_agent`, agent name |
| Tool ID present | `execute_tool`, tool name, call ID, arguments |
| Planning model present | `chat`, request model, usage, messages |
| Other spans | Ordinary OTel spans, preserving their original names |
| Thread ID | `gen_ai.conversation.id` on classified spans |
| Root input/output | Agent input/output messages |
| Planning messages/response | Model input/output messages |

Agent classification takes precedence, then tool, then model. Names follow
`operation target`. `gen_ai.provider.name` is the custom value `snowflake.cortex`:
it identifies the serving platform, not an inferred underlying model vendor.
Span kinds preserve Snowflake's original perspective; server/internal spans are
not relabeled as client calls. Spans whose original
`RECORD.name` starts with `ReasoningAgentStepPlanning-` have `-1` appended to their
exported span ID because Cortex can reuse that ID for a tool span. These planning
spans are leaves: parent IDs remain unchanged. Original IDs remain in
`trace_export.snowflake.row`, and raw exports are untouched. Span ID validation
accepts a nonzero 16-hex prefix with an optional suffix; trace IDs stay strict.
Suffixed IDs are not standard OTLP and may be rejected by strict consumers.
A zero parent ID is treated as absent. Nanosecond timestamps are preserved exactly;
timezone-free Snowflake timestamps are interpreted as UTC.

Source record attributes remain under their original names; resource attributes
are written on the OTLP Resource. Remaining raw columns (including `RECORD` and
`VALUE`) are stored in `trace_export.snowflake.row` on the span. This preserves
unmapped payloads for inspection. JSON numbers become OTLP int64/double values;
arbitrary decimal precision is guaranteed only in the raw export. Integers outside
int64 range are rejected rather than silently rounded.

Messages support plain text, role/content messages, JSON-encoded message arrays,
and Cortex string arrays prefixed with `User:`, `Assistant:`, `System:`, or `Tool:`.
Only the leading role prefix is removed; message order and embedded text are kept.
JSON-formatted response text is kept as one message unless it explicitly has a
structured message shape. Message content supports
text parts, tool calls and tool responses. Unsupported payload shapes are retained
in source attributes and reported as mapping warnings; no partial normalized
message list is emitted. The pinned output schema requires `finish_reason`, so
when it is unavailable the converter uses the explicit custom value `unknown`
and reports an aggregate conversion note. It never assumes `stop`. Conversation-history attributes are
retained but not substituted for a model call's actual input.

Tool argument objects are mapped directly. A scalar with an explicit argument
name or equal-length unique name/value arrays becomes an argument object. Other
shapes are retained with a warning. Tool results are not inferred from `VALUE`.

Token totals are emitted on model spans only, to avoid counting root aggregates
again. `--input-token-mode auto` copies the source input count when no nonzero
cache count is present. With nonzero cache counts, it also copies input when the
row corroborates inclusive accounting: `total = input + output` and
`input >= cache_read_input + cache_write_input`, with valid cache counts.
Otherwise it omits the ambiguous standard input total and reports a warning.
Explicit overrides remain available after confirming the source semantics:

- `--input-token-mode includes-cache`: source input is already the total.
- `--input-token-mode excludes-cache`: add reported cache-read/write counts once.

Original cache counts are retained under Snowflake attribute names. Missing counts
stay missing. The same option is supported by both export and standalone conversion.

Reversed time ranges are preserved as zero-duration spans: exported end equals
start, with `trace_export.timing.corrected = true` and
`trace_export.timing.reason = "end_precedes_start"`. Original timestamps remain
in `trace_export.snowflake.row`. Parent spans and relationships are unchanged.
The correction count is reported in the conversion summary. Corrected duration
means unknown actual duration and should be excluded from duration analysis.

Malformed IDs, missing timestamps, invalid JSON, or invalid
OTLP values fail the conversion with a line number and leave existing output
untouched. The standalone converter supports explicit `--skip-invalid` to retain
valid rows and report skips. Optional fields that cannot be mapped produce warnings
and remain in the original attributes. These warnings include line numbers for
the first three occurrences per category and a full count at the end.

Fixtures are synthetic examples of representative Cortex payload shapes, without
copied prompt content. Tests cover message schemas, OTLP wire structure,
hierarchy, timing correction, failure cleanup and raw-file retention. Downstream backend import remains untested.
