"""Langfuse v4 events_full rows (ClickHouse) -> OTLP envelopes, one span per row.

IDs and timestamps are used as stored; nothing is remapped or enriched. Langfuse
flattens span metadata into metadata_names/metadata_values: `attributes.<key>`
and `resourceAttributes.<key>` entries are restored onto the span and resource.
Langfuse already stringified span attribute values (and JSON-encoded non-string
resource values), so restored values are kept verbatim as strings; original
types are not guessed. The full source row, including all metadata, is retained.

GenAI v1.39.0 mapping follows the observation type: AGENT -> invoke_agent,
GENERATION -> chat, TOOL -> execute_tool; other types stay ordinary spans with
their original name. Restored `gen_ai.*` attributes take precedence over mapped
values. Two restored kinds are normalized in place: message lists that satisfy
the pinned message schemas become structured values, and in-range token count
digits become integers. Values that do not qualify stay verbatim strings with a
warning. Original stringified values always remain in the preserved row. No
provider is inferred, usage is the observation's own effective input/output
count (no trace totals or cache additions), and tool call IDs are never invented.
"""
import re

from .genai import SEMCONV_VERSION, decode, messages
from .otlp import attributes, envelope, identifier, unix_nano

MAPPING_VERSION = "1.0.0"
SCOPE_FALLBACK = "agentic-data-export.langfuse"
ROW_KEY = "trace_export.langfuse.row"
PROJECT_KEY = "langfuse.project.id"
# A credential hash, not trace data; dropped even if an external raw export has it.
EXCLUDED_COLUMNS = ("ingestion_api_key",)
METADATA_PREFIXES = ("attributes.", "resourceAttributes.")
INT64 = range(-(2**63), 2**63)
OPERATIONS = {"AGENT": "invoke_agent", "GENERATION": "chat", "TOOL": "execute_tool"}
NAME_SOURCES = {"invoke_agent": "gen_ai.agent.name", "chat": "gen_ai.request.model", "execute_tool": "gen_ai.tool.name"}
MESSAGE_KEYS = {"gen_ai.input.messages": False, "gen_ai.output.messages": True}
TOKEN_KEYS = {"input": "gen_ai.usage.input_tokens", "output": "gen_ai.usage.output_tokens"}
# Known part types in the pinned v1.39.0 message schemas: required string fields
# and optional fields that must be strings or null. Other types are generic parts.
PARTS = {"text": (("content",), ()), "reasoning": (("content",), ()),
         "tool_call": (("name",), ("id",)), "tool_call_response": ((), ("id",)),
         "blob": (("modality", "content"), ("mime_type",)), "uri": (("modality", "uri"), ("mime_type",)),
         "file": (("modality", "file_id"), ("mime_type",))}


def strict_id(value, length, label):
    # Unlike Cortex span IDs, Langfuse IDs carry no suffixes.
    if not isinstance(value, str) or not re.fullmatch(f"[0-9a-fA-F]{{{length}}}", value):
        raise ValueError(f"invalid {label}: expected {length} hexadecimal digits")
    return identifier(value, length, label)


def text(row, key, required=False):
    value = row.get(key)
    if value is None and not required:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def timestamp(row, key):
    if row.get(key) is None:
        raise ValueError(f"{key} missing")
    try:
        return unix_nano(row[key])
    except ValueError as exc:
        raise ValueError(f"{key}: {exc}") from None


def exact(value):
    # UInt64 values beyond int64, in the row or decoded payloads, keep their exact
    # decimal digits as strings; quoted ClickHouse decimals are already strings.
    if isinstance(value, int) and not isinstance(value, bool) and value not in INT64:
        return str(value)
    if isinstance(value, list):
        return [exact(item) for item in value]
    if isinstance(value, dict):
        return {key: exact(item) for key, item in value.items()}
    return value


def restored_metadata(row):
    names, values = row.get("metadata_names"), row.get("metadata_values")
    for key, items in (("metadata_names", names), ("metadata_values", values)):
        if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
            raise ValueError(f"{key} must be an array of strings")
    if len(names) != len(values):
        raise ValueError("metadata_names and metadata_values differ in length")
    restored = {prefix: {} for prefix in METADATA_PREFIXES}
    for name, value in zip(names, values):
        for prefix, target in restored.items():
            if name.startswith(prefix):
                key = name[len(prefix):]
                # Langfuse keeps the first duplicate; ambiguity is rejected here.
                if key in target:
                    raise ValueError(f"duplicate {prefix.rstrip('.')} key in metadata")
                target[key] = value
    return restored["attributes."], restored["resourceAttributes."]


def schema_messages(value, output):
    """Whether value satisfies the pinned input/output message schema, with known parts checked by type."""
    if not isinstance(value, list):
        return False
    for message in value:
        if (not isinstance(message, dict) or not isinstance(message.get("role"), str)
                or not isinstance(message.get("parts"), list) or not isinstance(message.get("name", ""), (str, type(None)))
                or (output and not isinstance(message.get("finish_reason"), str))):
            return False
        for part in message["parts"]:
            if not isinstance(part, dict) or not isinstance(part.get("type"), str):
                return False
            required, optional = PARTS.get(part["type"], ((), ()))
            if (not all(isinstance(part.get(field), str) for field in required)
                    or not all(isinstance(part.get(field), (str, type(None))) for field in optional)
                    or (part["type"] == "tool_call_response" and "response" not in part)):
                return False
    return True


def token_count(value):
    if isinstance(value, str) and re.fullmatch("[0-9]+", value):
        value = int(value)
    if isinstance(value, int) and not isinstance(value, bool) and 0 <= value < 2**63:
        return value
    return None


def normalize_restored(attrs, issues):
    for key, output in MESSAGE_KEYS.items():
        if key in attrs:
            value = decode(attrs[key])
            # Checked before exact(): schema fields must already be strings, so only
            # free-form content (such as tool arguments) can hold an integer to convert.
            if schema_messages(value, output):
                attrs[key] = exact(value)
            else:
                issues.append("existing_attribute_not_schema_compatible:" + key)
    for key in TOKEN_KEYS.values():
        if key in attrs:
            count = token_count(attrs[key])
            if count is None:
                issues.append("existing_attribute_not_schema_compatible:" + key)
            else:
                attrs[key] = count


def map_genai(row, span, attrs, issues):
    operation = OPERATIONS.get(text(row, "type"))
    if not operation:
        return
    attrs.setdefault("gen_ai.operation.name", operation)
    if operation == "invoke_agent":
        attrs.setdefault("gen_ai.agent.name", span["name"])
    elif operation == "execute_tool":
        attrs.setdefault("gen_ai.tool.name", span["name"])
        for column, target in (("input", "gen_ai.tool.call.arguments"), ("output", "gen_ai.tool.call.result")):
            if text(row, column):
                attrs.setdefault(target, exact(decode(row[column])))
    else:
        if text(row, "provided_model_name"):
            attrs.setdefault("gen_ai.request.model", row["provided_model_name"])
        usage = row.get("usage_details")
        if usage is not None and not isinstance(usage, dict):
            issues.append("token_usage_unmapped")
        elif usage:
            for source_key, target in TOKEN_KEYS.items():
                if source_key in usage and target not in attrs:
                    count = token_count(usage[source_key])
                    if count is None:
                        issues.append("invalid_token_count:" + source_key)
                    else:
                        attrs[target] = count
    if text(row, "session_id"):
        attrs.setdefault("gen_ai.conversation.id", row["session_id"])
    if operation != "execute_tool":
        for (target, output), column in zip(MESSAGE_KEYS.items(), ("input", "output")):
            if target not in attrs and text(row, column):
                warnings = []
                try:
                    attrs[target] = exact(messages(row[column], "assistant" if output else "user", output, warnings))
                    issues.extend(warnings)
                except (ValueError, TypeError, AttributeError):
                    issues.append("payload_unmapped:" + column)
    if text(row, "level", required=True) == "ERROR":
        attrs.setdefault("error.type", "_OTHER")
    target = attrs.get(NAME_SOURCES[operation])
    # Rename only when the target is known; otherwise keep the source name.
    if target:
        span["name"] = f"{attrs['gen_ai.operation.name']} {target}"


def add(values, key, value, label):
    if key in values:
        raise ValueError(f"{label} attribute {key} conflicts with a source attribute")
    values[key] = value


def convert_row(row):
    if not isinstance(row, dict):
        raise ValueError("row must be a JSON object")
    issues = []
    name = text(row, "name", required=True)
    if not name:
        raise ValueError("span name missing")
    span = {"traceId": strict_id(row.get("trace_id"), 32, "trace ID"),
            "spanId": strict_id(row.get("span_id"), 16, "span ID"),
            "name": name, "kind": 0,  # Langfuse does not retain the OTel span kind.
            "startTimeUnixNano": timestamp(row, "start_time"),
            "endTimeUnixNano": timestamp(row, "end_time")}
    parent = row.get("parent_span_id")
    if parent not in (None, "", "0000000000000000"):
        span["parentSpanId"] = strict_id(parent, 16, "parent span ID")
        if span["parentSpanId"] == span["spanId"]:
            raise ValueError("span is its own parent")
    attrs, resource = restored_metadata(row)
    normalize_restored(attrs, issues)
    map_genai(row, span, attrs, issues)
    project = text(row, "project_id", required=True)
    if not project:
        raise ValueError("project_id missing")
    add(resource, PROJECT_KEY, project, "resource")
    if int(span["endTimeUnixNano"]) < int(span["startTimeUnixNano"]):
        span["endTimeUnixNano"] = span["startTimeUnixNano"]
        add(attrs, "trace_export.timing.corrected", True, "span")
        add(attrs, "trace_export.timing.reason", "end_precedes_start", "span")
        issues.append("span_timing_corrected")
    add(attrs, ROW_KEY, exact({k: v for k, v in row.items() if k not in EXCLUDED_COLUMNS}), "span")
    span["status"] = {"code": 0}
    if text(row, "level", required=True) == "ERROR":
        span["status"] = {"code": 2}
        message = text(row, "status_message")
        if message:
            span["status"]["message"] = message
    span["attributes"] = attributes(attrs)
    scope = {"name": text(row, "scope_name") or SCOPE_FALLBACK,
             "attributes": attributes({"trace_export.gen_ai.semconv_version": SEMCONV_VERSION,
                                       "trace_export.mapping.version": MAPPING_VERSION})}
    if text(row, "scope_version"):
        scope["version"] = row["scope_version"]
    return envelope(span, resource, scope), issues
