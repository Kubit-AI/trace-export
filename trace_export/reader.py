"""Streaming helpers for formatters consuming converted OTLP JSONL."""
from dataclasses import dataclass
import json
import math
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parents[1] / 'schemas/otel-record-1.0.0.schema.json'


def finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('non-finite JSON number')
    return result


def reject_constant(value):
    raise ValueError('non-finite JSON number')


def decode_value(value):
    """Decode OTLP AnyValue. Bytes remain base64 strings; source null becomes None."""
    if not isinstance(value, dict) or len(value) > 1:
        raise ValueError('invalid OTLP AnyValue')
    if not value:
        return None
    key, item = next(iter(value.items()))
    if key == 'intValue':
        return int(item)
    if key == 'arrayValue':
        return [decode_value(v) for v in item.get('values', [])]
    if key == 'kvlistValue':
        return decode_attributes(item.get('values', []))
    if key not in ('stringValue', 'boolValue', 'doubleValue', 'bytesValue'):
        raise ValueError('unknown OTLP AnyValue type')
    return item


def decode_attributes(values):
    result = {}
    for pair in values:
        if pair['key'] in result:
            raise ValueError('duplicate OTLP attribute key')
        result[pair['key']] = decode_value(pair['value'])
    return result


def records(path):
    """Yield (1-based line number, envelope); reject blank/malformed records."""
    with Path(path).open(encoding='utf-8') as handle:
        for number, line in enumerate(handle, 1):
            try:
                record = json.loads(line, parse_constant=reject_constant, parse_float=finite_float)
                if not isinstance(record, dict) or not isinstance(record.get('resourceSpans'), list):
                    raise ValueError()
            except (ValueError, TypeError) as exc:
                raise ValueError(f'line {number}: expected an OTLP JSON object with resourceSpans') from exc
            yield number, record


@dataclass(frozen=True)
class SpanRecord:
    line_number: int
    span: dict
    resource: dict
    scope: dict
    resource_schema_url: str
    scope_schema_url: str

    @property
    def attributes(self):
        return decode_attributes(self.span.get('attributes', []))

    @property
    def resource_attributes(self):
        return decode_attributes(self.resource.get('attributes', []))

    @property
    def scope_attributes(self):
        return decode_attributes(self.scope.get('attributes', []))


def spans_in_record(record, line_number=1):
    for resource in record['resourceSpans']:
        for scope in resource['scopeSpans']:
            for span in scope['spans']:
                yield SpanRecord(line_number, span, resource.get('resource', {}), scope.get('scope', {}),
                                 resource.get('schemaUrl', ''), scope.get('schemaUrl', ''))


def iter_spans(path):
    """Stream spans with original OTLP fields and decoded attribute accessors.

    Empty envelopes yield no spans. No grouping/deduplication or validation beyond
    JSON structure is performed; use trace_export.validate for untrusted input.
    """
    for number, record in records(path):
        try:
            yield from spans_in_record(record, number)
        except (KeyError, TypeError) as exc:
            raise ValueError(f'line {number}: malformed OTLP envelope') from exc
