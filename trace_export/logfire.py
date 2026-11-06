"""Pydantic Logfire `records` rows -> OTLP envelopes, one span per row.

Logfire stores OpenTelemetry spans almost verbatim, so the mapping is direct:
IDs, timestamps, attributes, events, links, status, scope and resource come
from their columns unchanged. Logfire moves a few SDK attributes into columns
(`logfire.span_type`, `logfire.msg`, `logfire.level_num`, `logfire.tags`,
`logfire.json_schema`); they are restored onto the span. Columns derived from
other fields (`service_*`, `http_*`, `url_*`, `exception_*`, `duration`, ...)
are not repeated. `--format raw` keeps every column.

Pydantic AI already emits GenAI semantic-convention attributes. The only
normalization: `tool_call_response` parts in `gen_ai.input.messages` and
`gen_ai.output.messages` carry their payload under `result`, which v1.39.0
names `response`; it is renamed. Messages captured without content (Pydantic
AI `include_content=False`) are kept as stored, with a warning.

Logfire does not store the OTel span kind; it is exported as UNSPECIFIED.
Pending-span placeholder rows are skipped.
"""
import json

from .genai import SEMCONV_VERSION
from .langfuse import exact, schema_messages, strict_id
from .otlp import attributes, envelope, unix_nano

MAPPING_VERSION = "1.0.0"
SCOPE_FALLBACK = "agentic-data-export.logfire"
PROJECT_KEY = "logfire.project.id"
LINKS_KEY = "trace_export.logfire.otel_links"
STATUS = {None: 0, "": 0, "UNSET": 0, "OK": 1, "ERROR": 2}
MESSAGE_KEYS = {"gen_ai.input.messages": False, "gen_ai.output.messages": True}


def text(row, key, required=False):
    value = row.get(key)
    if value is None and not required:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def mapping(row, key):
    value = row.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a JSON object")
    return exact(value)


def timestamp(value, label):
    try:
        return unix_nano(value)
    except ValueError as exc:
        raise ValueError(f"{label}: {exc}") from None


def normalize_messages(attrs, issues):
    for key, output in MESSAGE_KEYS.items():
        value = attrs.get(key)
        if value is None:
            continue
        if isinstance(value, list):
            for message in value:
                for part in (message.get("parts") or []) if isinstance(message, dict) else []:
                    if (isinstance(part, dict) and part.get("type") == "tool_call_response"
                            and "response" not in part and "result" in part):
                        part["response"] = part.pop("result")
        if not schema_messages(value, output):
            issues.append("gen_ai_messages_not_schema_compatible:" + key)


def event(item):
    if not isinstance(item, dict) or not isinstance(item.get("event_name"), str):
        raise ValueError("otel_events entry missing event_name")
    result = {"timeUnixNano": timestamp(item.get("event_timestamp"), "event_timestamp"),
              "name": item["event_name"]}
    values = item.get("attributes") or {}
    if not isinstance(values, dict):
        raise ValueError("otel_events attributes must be a JSON object")
    if values:
        result["attributes"] = attributes(exact(values))
    return result


def link(item):
    # Accept both flat and OTel SDK-style nested `context` shapes.
    context = item.get("context") if isinstance(item.get("context"), dict) else item
    result = {"traceId": strict_id(context.get("trace_id"), 32, "link trace ID"),
              "spanId": strict_id(context.get("span_id"), 16, "link span ID")}
    values = item.get("attributes") or {}
    if not isinstance(values, dict):
        raise ValueError("link attributes must be a JSON object")
    if values:
        result["attributes"] = attributes(exact(values))
    return result


def convert_row(row):
    if not isinstance(row, dict):
        raise ValueError("row must be a JSON object")
    kind = text(row, "kind")
    if kind == "pending_span":
        return None, []
    issues = []
    name = text(row, "span_name", required=True)
    if not name:
        raise ValueError("span_name missing")
    start = timestamp(row.get("start_timestamp"), "start_timestamp")
    end = timestamp(row["end_timestamp"], "end_timestamp") if row.get("end_timestamp") is not None else start
    span = {"traceId": strict_id(row.get("trace_id"), 32, "trace ID"),
            "spanId": strict_id(row.get("span_id"), 16, "span ID"),
            "name": name, "kind": 0,  # Logfire does not store the OTel span kind.
            "startTimeUnixNano": start, "endTimeUnixNano": end}
    parent = row.get("parent_span_id")
    if parent not in (None, "", "0000000000000000"):
        span["parentSpanId"] = strict_id(parent, 16, "parent span ID")
        if span["parentSpanId"] == span["spanId"]:
            raise ValueError("span is its own parent")

    attrs = mapping(row, "attributes")
    # Restore SDK attributes Logfire moved into columns; source attributes win.
    if kind:
        attrs.setdefault("logfire.span_type", kind)
    if text(row, "message"):
        attrs.setdefault("logfire.msg", row["message"])
    if row.get("level") is not None:
        attrs.setdefault("logfire.level_num", row["level"])
    if row.get("tags"):
        attrs.setdefault("logfire.tags", row["tags"])
    if row.get("attributes_json_schema") is not None:
        # The SDK sends the schema as a JSON string attribute.
        attrs.setdefault("logfire.json_schema", json.dumps(row["attributes_json_schema"], separators=(",", ":")))
    if row.get("log_body") is not None:
        attrs.setdefault("trace_export.logfire.log_body", exact(row["log_body"]))
    normalize_messages(attrs, issues)

    status = text(row, "otel_status_code")
    if status not in STATUS:
        raise ValueError("unsupported otel_status_code")
    span["status"] = {"code": STATUS[status]}
    if span["status"]["code"] == 2:
        if text(row, "otel_status_message"):
            span["status"]["message"] = row["otel_status_message"]
        attrs.setdefault("error.type", text(row, "exception_type") or "_OTHER")

    if int(end) < int(start):
        span["endTimeUnixNano"] = start
        attrs["trace_export.timing.corrected"] = True
        attrs["trace_export.timing.reason"] = "end_precedes_start"
        issues.append("span_timing_corrected")

    events = row.get("otel_events") or []
    if not isinstance(events, list):
        raise ValueError("otel_events must be an array")
    if events:
        span["events"] = [event(item) for item in events]
    links = row.get("otel_links") or []
    if not isinstance(links, list):
        raise ValueError("otel_links must be an array")
    if links:
        try:
            span["links"] = [link(item) for item in links]
        except (ValueError, TypeError, AttributeError):
            # Unknown link shape: keep it verbatim rather than lose it.
            attrs.setdefault(LINKS_KEY, exact(links))
            issues.append("links_unmapped")
    span["attributes"] = attributes(attrs)

    resource = mapping(row, "otel_resource_attributes")
    if text(row, "project_id"):
        if resource.setdefault(PROJECT_KEY, row["project_id"]) != row["project_id"]:
            raise ValueError(f"resource attribute {PROJECT_KEY} conflicts with project_id")
    scope_attrs = mapping(row, "otel_scope_attributes")
    scope_attrs["trace_export.gen_ai.semconv_version"] = SEMCONV_VERSION
    scope_attrs["trace_export.mapping.version"] = MAPPING_VERSION
    scope = {"name": text(row, "otel_scope_name") or SCOPE_FALLBACK, "attributes": attributes(scope_attrs)}
    if text(row, "otel_scope_version"):
        scope["version"] = row["otel_scope_version"]
    return envelope(span, resource, scope), issues
