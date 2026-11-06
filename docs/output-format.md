# Converted JSONL contract, version 1.0.0

The shared output is newline-delimited OTLP JSON: each nonblank UTF-8 line is one
object with a `resourceSpans` array. The file has no header, schema line, or outer
array. The schema describes **one record**, not an entire JSONL file.

- [JSON Schema](../schemas/otel-record-1.0.0.schema.json), Draft 2020-12.
- Synthetic [Snowflake example](../examples/snowflake.otel.jsonl).
- Synthetic [Databricks example](../examples/databricks.otel.jsonl).
- Synthetic [ClickHouse (Langfuse) example](../examples/clickhouse.otel.jsonl).
- Synthetic [Pydantic Logfire example](../examples/logfire.otel.jsonl).

These examples are generated from test fixtures, not downloaded customer traces.

One converted record corresponds to one source database row. Snowflake SPAN rows,
ClickHouse Langfuse rows and Logfire rows contain one span each; Databricks unified rows contain zero or more spans, possibly
in multiple resource/scope groups. Snowflake non-SPAN rows and Logfire pending-span placeholders are omitted during
conversion. Empty Databricks traces retain an empty envelope. **A line is not a
promise of a complete trace.** Group across records by trace ID when needed;
missing parents can reflect export time filters or limits. Langfuse trace IDs are
unique only within a project: group ClickHouse records by the resource attribute
`langfuse.project.id` and trace ID when a file spans several projects.

This is a versioned contract for this exporter's OTLP subset, not a universal
schema for every possible OTLP producer. Unknown structural fields are rejected;
arbitrary attribute keys and nested values are allowed. Raw exports remain
source-specific and are not covered by this contract.

## Encoding and meaning

Trace IDs are nonzero 32-character hexadecimal strings. Span IDs (including
parent and link IDs) must start with 16 hexadecimal characters representing a
nonzero ID; suffixes are allowed. Cortex spans whose original names start with
`ReasoningAgentStepPlanning-` receive a `-1` suffix to distinguish them from tool
spans that reuse the same ID. Parent IDs and raw source IDs are unchanged, since
these planning spans have no children. **Suffixed IDs are an exporter extension,
not standard OTLP**, and strict OpenTelemetry consumers may reject them.
ClickHouse and Logfire output never use suffixes: their IDs must already be standard hexadecimal
IDs, and rows with other IDs fail conversion rather than being remapped.
A root omits `parentSpanId`. Nanosecond times
and int64 attribute values are decimal **strings** to preserve precision. Enums
such as kind/status are numbers. Resource/scope/span attributes are arrays of
`{"key": "...", "value": {"stringValue": "..."}}` entries. Other value types use
`boolValue`, `intValue`, `doubleValue`, `bytesValue`, `arrayValue`, or `kvlistValue`.
An empty value object `{}` preserves a source null. Bytes are base64 strings.

Original attributes and source metadata are retained alongside normalized
`gen_ai.*` attributes. GenAI fields are optional: not every span is an LLM, tool,
or agent invocation. Unmapped payloads remain available under source attributes.
ClickHouse span attributes restored from Langfuse metadata are strings, because
Langfuse stringified them at ingestion; see the [ClickHouse mapping](../clickhouse/README.md#mapping).
Reversed source timestamps become zero-duration spans with
`trace_export.timing.corrected=true`; the original source fields remain available.

Every scope carries the `trace_export.gen_ai.semconv_version` attribute. The
mapping version is the `trace_export.mapping.version` scope attribute for
Databricks, ClickHouse and Logfire, where scope `version` keeps any original source value;
Snowflake records it as the scope `version` instead. The semantic conventions
version is currently `1.39.0` and is independent of the JSONL contract version.
`schemaUrl` is reserved for the original OpenTelemetry resource/scope schema, not
the JSON Schema document.

## Validate locally

```sh
python -m pip install -r requirements-validation.txt
python -m trace_export.validate examples/databricks.otel.jsonl
python -m trace_export.validate data/export.jsonl --json
python -m trace_export.validate --schema
```

Validation streams one record at a time and stops at the first failure. Exit codes
are 0 (valid), 1 (invalid input/runtime/dependency failure), and 2 (usage error).
Errors identify the line and structural location without quoting trace payloads.
The validator checks the schema, duplicate attribute keys, parent self-references,
and timestamp ordering. It does not require parents to exist in the file, prove
trace completeness, or validate all GenAI semantics. Pinned GenAI message schemas
remain in `snowflake/tests/fixtures/semconv-v1.39.0/` and are exercised by converter
tests. OTLP protobuf tests check standard-ID converter output; suffixed planning
IDs are validated against this exporter's relaxed contract instead.

## Write a formatter

The reader has no extra dependencies. Run from the repository root, or put that
root on `PYTHONPATH`:

```python
from trace_export.reader import iter_spans

for item in iter_spans("data/export.jsonl"):
    span = item.span  # original OTLP fields, including events and links
    attrs = item.attributes  # decoded recursively to Python values
    resource = item.resource_attributes
    scope = item.scope_attributes
    print(span["traceId"], span["spanId"], attrs.get("gen_ai.operation.name"))
```

Each item includes `line_number`, the original `resource` and `scope` objects,
`resource_schema_url`, and `scope_schema_url`. Integer attributes become Python
integers; source nulls become `None`; bytes remain base64 strings. Span timestamps
remain original decimal strings: use `int(...)` before calculating durations.
`decode_attributes` and `decode_value` also work on event/link attributes. Duplicate
keys raise an error instead of silently discarding values.

`iter_spans` streams without regrouping or deduplicating spans and skips empty
span arrays. Use `records(path)` if you need to preserve empty database rows.
Validate inputs separately; the reader is a parsing helper, not a schema validator.
Formatters should be separate from warehouse downloads, so saved exports can be
reformatted without credentials or repeat queries.

Changes to this contract get a new schema file/version; existing versioned schema
files should remain available. See the [OTLP specification](https://opentelemetry.io/docs/specs/otlp/)
for the underlying encoding.
