"""Databricks MLflow unified rows -> GenAI OTLP envelopes.

Each envelope retains every span from one database row. Trace metadata is kept
once on the scope, and source span fields are retained on their own span.
"""
import json

from .genai import SEMCONV_VERSION, decode, messages
from .otlp import attributes, identifier

MAPPING_VERSION = "1.0.0"
STATUS = {"STATUS_CODE_UNSET": 0, "UNSET": 0, "STATUS_CODE_OK": 1, "OK": 1,
          "STATUS_CODE_ERROR": 2, "ERROR": 2}
KINDS = {"SPAN_KIND_UNSPECIFIED": 0, "SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2,
         "SPAN_KIND_CLIENT": 3, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5}


def trace_id(value):
    if isinstance(value, str):
        if value.startswith("trace:/"):
            value = value.rsplit("/", 1)[-1]
        value = value.removeprefix("tr-")
    return identifier(value, 32, "trace ID")


def nanos(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).isdigit():
        raise ValueError("span timestamp must be integer nanoseconds")
    value = int(value)
    if not 0 <= value < 2**64:
        raise ValueError("span timestamp outside OTLP uint64 range")
    return str(value)


def object_value(value, label):
    value = decode(value)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def message_payload(value, output, issues):
    """Unwrap MLflow function inputs and OpenAI completion outputs."""
    decoded = decode(value)
    if isinstance(decoded, dict) and output and "choices" in decoded:
        choices = decoded["choices"]
        if not isinstance(choices, list):
            raise ValueError("choices must be an array")
        result = []
        for choice in choices:
            if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
                raise ValueError("choice missing message")
            message = dict(choice["message"])
            if choice.get("finish_reason") is not None:
                message["finish_reason"] = choice["finish_reason"]
            result.extend(messages(message, "assistant", True, issues))
        return result
    if isinstance(decoded, dict) and "messages" in decoded:
        return messages(decoded["messages"], "assistant" if output else "user", output, issues)
    if isinstance(decoded, dict):
        field = "output" if output else "input"
        if isinstance(decoded.get(field), list):
            return messages(decoded[field], "assistant" if output else "user", output, issues)
        if output and ("content" in decoded or "tool_calls" in decoded):
            return messages({"role": "assistant", **decoded}, "assistant", True, issues)
    if isinstance(decoded, dict) and not output and isinstance(decoded.get("inputs"), dict) and "messages" in decoded["inputs"]:
        return messages(decoded["inputs"]["messages"], "user", False, issues)
    return messages(value, "assistant" if output else "user", output, issues)


def convert_span(source, row, metadata, issues):
    if not isinstance(source, dict):
        raise ValueError("span must be an object")
    name = source.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("span name missing")
    row_id = trace_id(row.get("trace_id"))
    if source.get("trace_id") and trace_id(source["trace_id"]) != row_id:
        raise ValueError("span trace ID differs from row trace ID")
    span = {"traceId": row_id, "spanId": identifier(source.get("span_id"), 16, "span ID"),
            "name": name, "startTimeUnixNano": nanos(source.get("start_time_unix_nano")),
            "endTimeUnixNano": nanos(source.get("end_time_unix_nano"))}
    parent = source.get("parent_span_id")
    if parent not in (None, "", "0000000000000000"):
        span["parentSpanId"] = identifier(parent, 16, "parent span ID")
        if span["spanId"] == span["parentSpanId"]:
            raise ValueError("span is its own parent")
    raw_attrs = object_value(source.get("attributes"), "span attributes")
    attrs = dict(raw_attrs)
    attrs["trace_export.databricks.span"] = {k: v for k, v in source.items() if k != "attributes"}
    if int(span["endTimeUnixNano"]) < int(span["startTimeUnixNano"]):
        span["endTimeUnixNano"] = span["startTimeUnixNano"]
        attrs.update({"trace_export.timing.corrected": True, "trace_export.timing.reason": "end_precedes_start"})
        issues.append("span_timing_corrected")
    kind = source.get("kind", "SPAN_KIND_INTERNAL")
    if kind is None:
        kind = "SPAN_KIND_INTERNAL"
    if isinstance(kind, int) and not isinstance(kind, bool) and 0 <= kind <= 5:
        span["kind"] = kind
    elif isinstance(kind, str) and kind in KINDS:
        span["kind"] = KINDS[kind]
    else:
        raise ValueError("unrecognized span kind")
    status = source.get("status") or {}
    if isinstance(status, str):
        status = {"code": status}
    if not isinstance(status, dict):
        raise ValueError("invalid span status")
    code = status.get("code", "STATUS_CODE_UNSET")
    if isinstance(code, str) and code in STATUS:
        code = STATUS[code]
    if isinstance(code, bool) or not isinstance(code, int) or code not in (0, 1, 2):
        raise ValueError("unrecognized span status")
    span["status"] = {"code": code}
    if code == 2:
        attrs.setdefault("error.type", "_OTHER")
        if isinstance(status.get("message"), str):
            span["status"]["message"] = status["message"]

    span_type = decode(raw_attrs.get("mlflow.spanType"))
    operation = {"AGENT": "invoke_agent", "CHAT_MODEL": "chat", "LLM": "chat", "TOOL": "execute_tool"}.get(span_type)
    if operation:
        attrs.setdefault("gen_ai.operation.name", operation)
        # MLflow is an instrumentation framework, not a model provider.
        provider = decode(raw_attrs.get("mlflow.llm.provider"))
        if isinstance(provider, str):
            attrs.setdefault("gen_ai.provider.name", provider)
        if operation == "invoke_agent":
            attrs.setdefault("gen_ai.agent.name", name)
        elif operation == "chat":
            model = decode(raw_attrs.get("mlflow.llm.model"))
            if isinstance(model, str):
                attrs.setdefault("gen_ai.request.model", model)
            usage = decode(raw_attrs.get("mlflow.chat.tokenUsage"))
            if usage is not None and not isinstance(usage, dict):
                issues.append("token_usage_unmapped")
            elif usage:
                for source_key, target in (("input_tokens", "gen_ai.usage.input_tokens"), ("output_tokens", "gen_ai.usage.output_tokens")):
                    value = usage.get(source_key)
                    if value is not None:
                        if isinstance(value, bool) or not str(value).isdigit() or not 0 <= int(value) < 2**63:
                            issues.append("invalid_token_count:" + source_key)
                        else:
                            attrs.setdefault(target, int(value))
        else:
            attrs.setdefault("gen_ai.tool.name", decode(raw_attrs.get("tool.name")) or name)
            call_id = decode(raw_attrs.get("tool_call_id", raw_attrs.get("tool.id")))
            if isinstance(call_id, str):
                attrs.setdefault("gen_ai.tool.call.id", call_id)
            for field, target in (("mlflow.spanInputs", "gen_ai.tool.call.arguments"), ("mlflow.spanOutputs", "gen_ai.tool.call.result")):
                if raw_attrs.get(field) is not None:
                    attrs.setdefault(target, decode(raw_attrs[field]))
        session = decode(metadata.get("mlflow.trace.session"))
        if session is not None:
            attrs.setdefault("gen_ai.conversation.id", str(session))
        target = attrs.get({"invoke_agent": "gen_ai.agent.name", "chat": "gen_ai.request.model", "execute_tool": "gen_ai.tool.name"}[operation])
        span["name"] = str(attrs["gen_ai.operation.name"]) + (f" {target}" if target else "")

    if operation in ("invoke_agent", "chat") or "parentSpanId" not in span:
        for field, target, output in (("mlflow.spanInputs", "gen_ai.input.messages", False), ("mlflow.spanOutputs", "gen_ai.output.messages", True)):
            if target in attrs:
                continue
            value = raw_attrs.get(field)
            if value is None and "parentSpanId" not in span:
                value = row.get("response" if output else "request")
            if value is not None:
                try:
                    attrs[target] = message_payload(value, output, issues)
                except (ValueError, TypeError, AttributeError):
                    issues.append("payload_unmapped:" + field)
    span["attributes"] = attributes(attrs)
    for field, target in (("trace_state", "traceState"), ("flags", "flags"),
                          ("dropped_attributes_count", "droppedAttributesCount"),
                          ("dropped_events_count", "droppedEventsCount"), ("dropped_links_count", "droppedLinksCount")):
        if source.get(field) is not None:
            span[target] = source[field]
    if source.get("events"):
        span["events"] = []
        for event in source["events"]:
            item = {"timeUnixNano": nanos(event["time_unix_nano"]), "name": event["name"],
                    "attributes": attributes(object_value(event.get("attributes"), "event attributes"))}
            if event.get("dropped_attributes_count") is not None:
                item["droppedAttributesCount"] = event["dropped_attributes_count"]
            span["events"].append(item)
    if source.get("links"):
        span["links"] = []
        for link in source["links"]:
            item = {"traceId": trace_id(link["trace_id"]), "spanId": identifier(link["span_id"], 16, "link span ID"),
                    "attributes": attributes(object_value(link.get("attributes"), "link attributes"))}
            for field, target in (("trace_state", "traceState"), ("flags", "flags"), ("dropped_attributes_count", "droppedAttributesCount")):
                if link.get(field) is not None:
                    item[target] = link[field]
            span["links"].append(item)
    return span


def resource_scope(source, row):
    resource = object_value(source.get("resource"), "resource")
    resource_attrs = dict(object_value(resource.get("attributes"), "resource attributes"))
    if source.get("service_name"):
        resource_attrs.setdefault("service.name", source["service_name"])
    resource_wire = {"attributes": attributes(resource_attrs)}
    if resource.get("dropped_attributes_count") is not None:
        resource_wire["droppedAttributesCount"] = resource["dropped_attributes_count"]
    original_scope = object_value(source.get("instrumentation_scope"), "instrumentation scope")
    scope_attrs = dict(object_value(original_scope.get("attributes"), "scope attributes"))
    scope_attrs.update({"trace_export.gen_ai.semconv_version": SEMCONV_VERSION,
                       "trace_export.mapping.version": MAPPING_VERSION,
                       "trace_export.databricks.row": {k: v for k, v in row.items() if k != "spans"}})
    scope = {"name": original_scope.get("name") or "agentic-data-export.databricks",
             "attributes": attributes(scope_attrs)}
    if original_scope.get("version"):
        scope["version"] = original_scope["version"]
    if original_scope.get("dropped_attributes_count") is not None:
        scope["droppedAttributesCount"] = original_scope["dropped_attributes_count"]
    group = {"resource": resource_wire, "scopeSpans": [{"scope": scope, "spans": []}]}
    if source.get("resource_schema_url"):
        group["schemaUrl"] = source["resource_schema_url"]
    if source.get("span_schema_url"):
        group["scopeSpans"][0]["schemaUrl"] = source["span_schema_url"]
    return group


def convert_row(row):
    if not isinstance(row, dict) or not isinstance(row.get("spans"), list):
        raise ValueError("unified row must contain a spans array")
    metadata = object_value(row.get("trace_metadata"), "trace metadata")
    groups, issues, seen = {}, [], set()
    for index, source in enumerate(row["spans"]):
        try:
            span = convert_span(source, row, metadata, issues)
            if span["spanId"] in seen:
                raise ValueError("duplicate span ID in row")
            seen.add(span["spanId"])
            group = resource_scope(source, row)
            key = json.dumps(group, sort_keys=True)
            groups.setdefault(key, group)["scopeSpans"][0]["spans"].append(span)
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError(f"span {index + 1}: {exc}") from exc
    # One envelope per source row, even for a row with no spans.
    return {"resourceSpans": list(groups.values()) or [resource_scope({}, row)]}, issues
