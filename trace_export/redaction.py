"""Best-effort missing-content warning for Snowflake raw exports."""
import json

PREFIX = 'snow.ai.observability.agent.'
CONTENT_KEYS = {
    'ai.observability.record_root.input', 'ai.observability.record_root.output',
    *(PREFIX + key for key in ('messages', 'response', 'planning.messages',
                               'planning.response', 'thread_id.messages')),
    'gen_ai.input.messages', 'gen_ai.output.messages',
    'gen_ai.tool.call.arguments', 'gen_ai.tool.call.result',
}
TOOL_FIELDS = ('input', 'inputs', 'output', 'outputs', 'arguments', 'argument.value',
               'result', 'results', 'response', 'request', 'query', 'sql', 'code')
WARNING = {
    'code': 'snowflake_payloads_empty_possible_redaction',
    'message': 'Rows were returned, but all recognized conversation, tool input/output, '
               'and user-feedback content is empty or absent. Content may be redacted or '
               'genuinely empty, including in a small sample. Full content requires '
               'READ UNREDACTED AI OBSERVABILITY EVENTS TABLE ON ACCOUNT in addition to '
               'agent access. Ask your Snowflake administrator to verify effective privileges; '
               'see snowflake/README.md#permissions-and-unredacted-traces.',
}


def has_content(value, message=False):
    if value is None:
        return False
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return False
        try:
            decoded = json.loads(value)
        except ValueError:
            return True
        # Do not recursively decode string literals indefinitely.
        if isinstance(decoded, str):
            return bool(decoded.strip())
        return has_content(decoded, message)
    if isinstance(value, dict):
        ignored = {'role', 'type', 'id', 'name', 'tool_call_id', 'finish_reason', 'index'} if message else set()
        return any(has_content(v, message) for k, v in value.items() if k not in ignored)
    if isinstance(value, list):
        return any(has_content(v, message) for v in value)
    # False and zero can be real tool results or user feedback.
    return True


def row_has_content(row):
    attrs = row.get('RECORD_ATTRIBUTES') or {}
    if isinstance(attrs, dict):
        for key, value in attrs.items():
            is_tool_payload = key.startswith(PREFIX + 'tool.') and any(
                key.endswith('.' + field) for field in TOOL_FIELDS)
            is_feedback = (key == PREFIX + 'feedback' or key.startswith(PREFIX + 'feedback.')) and not key.endswith(('.id', '.timestamp', '.request_id'))
            is_message = key in CONTENT_KEYS and not key.startswith('gen_ai.tool.')
            if (key in CONTENT_KEYS or is_tool_payload or is_feedback) and has_content(value, message=is_message):
                return True
    # Feedback event bodies may live in VALUE rather than span attributes.
    record = row.get('RECORD') or {}
    if isinstance(record, dict) and 'feedback' in str(record.get('name', '')).lower():
        return has_content(row.get('VALUE'))
    return False


def export_warnings(path, row_count):
    if not row_count:
        return []
    with path.open(encoding='utf-8') as handle:
        for line in handle:
            if row_has_content(json.loads(line, parse_float=str)):
                return []
    return [dict(WARNING)]
