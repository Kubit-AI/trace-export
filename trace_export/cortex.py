"""Cortex SPAN rows -> OTLP + GenAI semantic conventions v1.39.0.

Source attributes remain available; normalization adds attributes without
inventing missing model names or successful outcomes. Planning IDs receive a
suffix to distinguish them from tool spans that reuse the same source ID.
"""

from .genai import SEMCONV_VERSION, decode, messages
from .otlp import attributes, envelope, identifier, unix_nano

MAPPING_VERSION = "1.2.0"
PREFIX = "snow.ai.observability."
KINDS = {"SPAN_KIND_UNSPECIFIED": 0, "SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2,
         "SPAN_KIND_CLIENT": 3, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5}
STATUSES = {"STATUS_CODE_UNSET": 0, "STATUS_CODE_OK": 1, "STATUS_CODE_ERROR": 2}


def object_column(row, key):
    value = row.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return value


def convert_row(row, input_token_mode="auto"):
    if not isinstance(row, dict):
        raise ValueError("row must be a JSON object")
    if row.get("RECORD_TYPE") != "SPAN":
        if row.get("RECORD_TYPE") in ("LOG", "SPAN_EVENT", "METRIC"):
            return None, []
        raise ValueError("missing or unsupported RECORD_TYPE")
    warnings = []
    record, trace, source, resource = [object_column(row, key) for key in
                                     ("RECORD", "TRACE", "RECORD_ATTRIBUTES", "RESOURCE_ATTRIBUTES")]
    get = lambda key: source.get(PREFIX + key)
    name = record.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("span name missing")
    span = {"traceId": identifier(trace.get("trace_id"), 32, "trace ID"),
            "spanId": identifier(trace.get("span_id"), 16, "span ID"),
            "name": name, "startTimeUnixNano": unix_nano(row.get("START_TIMESTAMP")),
            "endTimeUnixNano": unix_nano(row.get("TIMESTAMP"))}
    # Cortex can reuse a planning span ID for a tool span. Planning spans are
    # leaves, so parent IDs stay unchanged and identify non-planning spans.
    if name.startswith("ReasoningAgentStepPlanning-"):
        span["spanId"] += "-1"
    timing_corrected = int(span["endTimeUnixNano"]) < int(span["startTimeUnixNano"])
    if timing_corrected:
        span["endTimeUnixNano"] = span["startTimeUnixNano"]
        warnings.append("span_timing_corrected")
    parent = record.get("parent_span_id")
    if parent not in (None, "", "0000000000000000"):
        span["parentSpanId"] = identifier(parent, 16, "parent span ID")
        if span["parentSpanId"] == span["spanId"]:
            raise ValueError("span is its own parent")
    kind = record.get("kind", "SPAN_KIND_UNSPECIFIED")
    if kind not in KINDS:
        raise ValueError("unrecognized span kind")
    span["kind"] = KINDS[kind]
    status = record.get("status") or {}
    if isinstance(status, str):
        status = {"code": status}
    if not isinstance(status, dict) or status.get("code", "STATUS_CODE_UNSET") not in STATUSES:
        raise ValueError("unrecognized span status")
    span["status"] = {"code": STATUSES[status.get("code", "STATUS_CODE_UNSET")]}
    attrs = dict(source)
    if timing_corrected:
        attrs["trace_export.timing.corrected"] = True
        attrs["trace_export.timing.reason"] = "end_precedes_start"
    # Preserve original record details, extra columns and VALUE without mixing
    # their keys with vendor attributes. Resource attributes live on Resource.
    attrs["trace_export.snowflake.row"] = {k: v for k, v in row.items() if k not in
                                           ("RECORD_ATTRIBUTES", "RESOURCE_ATTRIBUTES")}
    if span["status"]["code"] == 2:
        attrs["error.type"] = "_OTHER"
        description = status.get("message") or get("agent.planning.status.description") or get("agent.status.description")
        if isinstance(description, str):
            span["status"]["message"] = description
    operation = "invoke_agent" if name == "Agent" else (
        "execute_tool" if get("agent.tool.id") is not None else "chat" if get("agent.planning.model") is not None else None)
    if operation:
        attrs.update({"gen_ai.operation.name": operation, "gen_ai.provider.name": "snowflake.cortex"})
        mapping = {"agent.thread_id": "gen_ai.conversation.id"}
        if operation == "invoke_agent":
            mapping["object.name"] = "gen_ai.agent.name"
        elif operation == "chat":
            mapping["agent.planning.model"] = "gen_ai.request.model"
        else:
            mapping["agent.tool.id"] = "gen_ai.tool.call.id"
            attrs["gen_ai.tool.name"] = str(get("agent.tool.code_execution.name") or name)
            arguments = get("agent.tool.code_execution.argument.value")
            if arguments is not None:
                arguments = decode(arguments)
                argument_name = get("agent.tool.code_execution.argument.name")
                if isinstance(arguments, dict):
                    attrs["gen_ai.tool.call.arguments"] = arguments
                elif isinstance(argument_name, str):
                    attrs["gen_ai.tool.call.arguments"] = {argument_name: arguments}
                elif isinstance(argument_name, list) and isinstance(arguments, list) and len(argument_name) == len(arguments) and all(isinstance(x, str) for x in argument_name) and len(set(argument_name)) == len(argument_name):
                    attrs["gen_ai.tool.call.arguments"] = dict(zip(argument_name, arguments))
                else:
                    warnings.append("tool_arguments_unmapped")
        for source_key, target in mapping.items():
            if get(source_key) is not None:
                attrs[target] = str(get(source_key))
        suffix = attrs.get({"invoke_agent": "gen_ai.agent.name", "chat": "gen_ai.request.model", "execute_tool": "gen_ai.tool.name"}[operation])
        span["name"] = operation + (f" {suffix}" if suffix else "")

    if operation == "chat":
        counts = {}
        invalid_cache = False
        for key in ("input", "output", "cache_read_input", "cache_write_input", "total"):
            value = get("agent.planning.token_count." + key)
            if value is None:
                continue
            if isinstance(value, bool) or not str(value).isdigit() or not 0 <= int(value) < 2**63:
                warnings.append("invalid_token_count:" + key)
                invalid_cache |= key in ("cache_read_input", "cache_write_input")
            else:
                counts[key] = int(value)
        if "output" in counts:
            attrs["gen_ai.usage.output_tokens"] = counts["output"]
        cache = counts.get("cache_read_input", 0) + counts.get("cache_write_input", 0)
        if "input" in counts:
            # The observed Cortex total already accounts for cached input.
            # Require row-level corroboration before applying that inference.
            inclusive_total = (not invalid_cache and "output" in counts and "total" in counts
                               and counts["total"] == counts["input"] + counts["output"]
                               and counts["input"] >= cache)
            if (input_token_mode == "auto" and cache and not inclusive_total) or (invalid_cache and input_token_mode != "includes-cache"):
                warnings.append("input_token_cache_semantics_unknown")
            else:
                attrs["gen_ai.usage.input_tokens"] = counts["input"] + (cache if input_token_mode == "excludes-cache" else 0)
        # Cache counts stay under original vendor keys for the pinned convention.

    if operation in ("invoke_agent", "chat"):
        input_keys = (["ai.observability.record_root.input", PREFIX + "agent.messages"] if operation == "invoke_agent"
                      else [PREFIX + "agent.planning.messages"])
        output_keys = (["ai.observability.record_root.output", PREFIX + "agent.response"] if operation == "invoke_agent"
                       else [PREFIX + "agent.planning.response"])
        for keys, target, role, output in ((input_keys, "gen_ai.input.messages", "user", False),
                                           (output_keys, "gen_ai.output.messages", "assistant", True)):
            for key in keys:
                if source.get(key) is not None:
                    try:
                        attrs[target] = messages(source[key], role, output, warnings)
                    except (ValueError, TypeError, AttributeError):
                        warnings.append("payload_unmapped:" + key)
                    break
    span["attributes"] = attributes(attrs)
    scope = {"name": "agentic-data-export.snowflake", "version": MAPPING_VERSION,
             "attributes": attributes({"trace_export.gen_ai.semconv_version": SEMCONV_VERSION,
                                       "trace_export.input_token_mode": input_token_mode})}
    return envelope(span, resource, scope), warnings
