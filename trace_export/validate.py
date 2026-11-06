"""Validate converted JSONL locally: python -m trace_export.validate FILE."""
import argparse
import json
from pathlib import Path
import sys
from .reader import SCHEMA_PATH, records, spans_in_record, decode_attributes


def validator():
    from jsonschema import Draft202012Validator
    return Draft202012Validator(json.loads(SCHEMA_PATH.read_text()))


def validate_record(record, schema_validator=None):
    check = schema_validator if schema_validator is not None else validator()
    error = next(check.iter_errors(record), None)
    if error:
        # Do not include attribute values / user content in diagnostics.
        location = '/'.join(str(p) for p in error.absolute_path)
        raise ValueError(f'schema violation ({error.validator}) at /{location}')
    for resource in record['resourceSpans']:
        decode_attributes(resource.get('resource', {}).get('attributes', []))
        for scope in resource['scopeSpans']:
            decode_attributes(scope.get('scope', {}).get('attributes', []))
    count = 0
    for item in spans_in_record(record):
        span = item.span
        if int(span['endTimeUnixNano']) < int(span['startTimeUnixNano']):
            raise ValueError('span end precedes start')
        if span.get('parentSpanId', '').lower() == span['spanId'].lower():
            raise ValueError('span is its own parent')
        decode_attributes(span.get('attributes', []))
        for child in span.get('events', []) + span.get('links', []):
            decode_attributes(child.get('attributes', []))
        count += 1
    return count


def main(argv=None):
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('input', type=Path, nargs='?')
    cli.add_argument('--schema', action='store_true', help='Print the JSON schema; no input needed')
    cli.add_argument('--json', action='store_true', help='Machine-readable validation summary')
    args = cli.parse_args(argv)
    if args.schema:
        print(SCHEMA_PATH.read_text(), end='')
        return 0
    if not args.input:
        cli.error('input is required unless --schema is used')
    rows = spans = 0
    try:
        check = validator()
        for number, record in records(args.input):
            try:
                spans += validate_record(record, check)
            except ValueError as exc:
                raise ValueError(f'line {number}: {exc}') from exc
            rows += 1
    except (ImportError, OSError, ValueError) as exc:
        message = 'Install validation dependencies: python -m pip install -r requirements-validation.txt' if isinstance(exc, ImportError) else str(exc)
        if args.json:
            print(json.dumps({'status':'error', 'rows_validated':rows, 'spans_validated':spans, 'error':message}))
        else:
            print(f'Validation failed: {message}', file=sys.stderr)
        return 1
    result = {'status':'ok', 'schema_version':'1.0.0', 'rows':rows, 'spans':spans}
    print(json.dumps(result) if args.json else f'Valid: {rows} records, {spans} spans (schema 1.0.0).')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
