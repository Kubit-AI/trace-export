"""Shared GenAI v1.39.0 message normalization."""
import json
import re

SEMCONV_VERSION = "1.39.0"


def decode(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            pass
    return value


def parts(content):
    if isinstance(content, str):
        return [{"type": "text", "content": content}]
    if not isinstance(content, list):
        raise ValueError("unsupported message content")
    result = []
    for part in content:
        if not isinstance(part, dict) or not isinstance(part.get("type"), str):
            raise ValueError("unsupported message part")
        kind = part["type"]
        call_id = part.get("id", part.get("tool_use_id"))
        if kind in ("tool_use", "tool_call", "tool_result", "tool_call_response") and call_id is not None and not isinstance(call_id, str):
            raise ValueError("tool call ID must be a string")
        if kind in ("text", "input_text", "output_text"):
            text = part.get("content", part.get("text"))
            if not isinstance(text, str):
                raise ValueError("invalid text part")
            result.append({"type": "text", "content": text})
        elif kind in ("tool_use", "tool_call"):
            if not isinstance(part.get("name"), str):
                raise ValueError("tool call missing name")
            result.append({"type": "tool_call", "name": part["name"],
                           "id": part.get("id"), "arguments": decode(part.get("arguments", part.get("input")))})
        elif kind in ("tool_result", "tool_call_response"):
            field = "response" if kind == "tool_call_response" else "content"
            if field not in part:
                raise ValueError("tool response missing content")
            result.append({"type": "tool_call_response", "id": part.get("id", part.get("tool_use_id")),
                           "response": part[field]})
        else:
            raise ValueError("unsupported message part type")
    return result
def messages(value, role, output, warnings):
    original = value
    value = decode(value)
    if isinstance(original, str):
        structured = (isinstance(value, dict) and ("role" in value or "messages" in value)) or (
            isinstance(value, list) and bool(value) and all(isinstance(m, dict) and "role" in m for m in value))
        # A response can itself be JSON (including an array of strings). It is
        # one message's text, not a list of generated messages or choices.
        if (output and not structured) or not isinstance(value, (list, dict)):
            value = original
    if isinstance(value, str):
        value = [{"role": role, "content": value}]
    if isinstance(value, dict):
        if "messages" in value:
            value = value["messages"]
        else:
            value = [value]
    if not isinstance(value, list):
        raise ValueError("unsupported messages payload")
    result = []
    for message in value:
        if isinstance(message, str) and not output:
            match = re.match(r"^(User|Assistant|System|Tool): ?", message)
            if not match:
                raise ValueError("string message missing recognized role prefix")
            message = {"role": match[1].lower(), "content": message[match.end():]}
        if not isinstance(message, dict) or not isinstance(message.get("role"), str):
            raise ValueError("message missing role")
        body = message.get("parts", message.get("content"))
        converted = parts(body) if body is not None else []
        for call in message.get("tool_calls", []):
            function = call.get("function", {})
            if not isinstance(function.get("name"), str):
                raise ValueError("tool call missing function name")
            if call.get("id") is not None and not isinstance(call["id"], str):
                raise ValueError("tool call ID must be a string")
            converted.append({"type": "tool_call", "id": call.get("id"),
                              "name": function["name"], "arguments": decode(function.get("arguments"))})
        if message["role"] == "tool" and "tool_call_id" in message:
            if not isinstance(message["tool_call_id"], str):
                raise ValueError("tool call ID must be a string")
            converted = [{"type": "tool_call_response", "id": message["tool_call_id"], "response": body}]
        if body is None and not converted:
            raise ValueError("message missing content")
        item = {"role": message["role"], "parts": converted}
        if output:
            reason = message.get("finish_reason")
            if not isinstance(reason, str):
                # Required by the pinned output schema; custom value explicitly
                # means unavailable, rather than assuming successful completion.
                reason = "unknown"
                warnings.append("output_finish_reason_unavailable")
            item["finish_reason"] = reason
        result.append(item)
    return result
