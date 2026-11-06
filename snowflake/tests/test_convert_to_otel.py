import base64
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stderr
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from trace_export.cortex import convert_row, messages, PREFIX
from trace_export.convert import main
from trace_export.otlp import unix_nano

FIXTURES = Path(__file__).parent / "fixtures"


def unpack(value):
    if "arrayValue" in value:
        return [unpack(v) for v in value["arrayValue"]["values"]]
    if "kvlistValue" in value:
        return attr_dict(value["kvlistValue"]["values"])
    if "intValue" in value:
        return int(value["intValue"])
    return next(iter(value.values()), None)


def attr_dict(values):
    return {v["key"]: unpack(v["value"]) for v in values}


def span_of(envelope):
    return envelope["resourceSpans"][0]["scopeSpans"][0]["spans"][0]


class ConversionTests(unittest.TestCase):
    def setUp(self):
        self.rows = json.loads((FIXTURES / "cortex_rows.json").read_text())

    def test_hierarchy_and_classification(self):
        spans = [span_of(convert_row(row)[0]) for row in self.rows[:4]]
        self.assertEqual([s['name'] for s in spans], ['invoke_agent TEST_AGENT', 'chat example-model', 'execute_tool weather', 'Internal checkpoint'])
        self.assertNotIn('parentSpanId', spans[0])
        self.assertEqual(spans[1]['parentSpanId'], spans[0]['spanId'])
        self.assertEqual(spans[2]['parentSpanId'], spans[1]['spanId'])
        self.assertNotIn('gen_ai.operation.name', attr_dict(spans[3]['attributes']))
        self.assertEqual(spans[0]['startTimeUnixNano'], '1790208000123456789')

    def test_planning_ids_are_suffixed_without_changing_tool_or_parent_ids(self):
        planning = copy.deepcopy(self.rows[1])
        planning['RECORD']['name'] = 'ReasoningAgentStepPlanning-0'
        tool = copy.deepcopy(self.rows[2])
        tool['TRACE']['span_id'] = planning['TRACE']['span_id']
        tool['RECORD']['parent_span_id'] = planning['RECORD']['parent_span_id']
        original = copy.deepcopy(planning)
        plan_span = span_of(convert_row(planning)[0])
        tool_span = span_of(convert_row(tool)[0])
        self.assertEqual(plan_span['spanId'], tool_span['spanId'] + '-1')
        self.assertEqual(plan_span['parentSpanId'], tool_span['parentSpanId'])
        self.assertEqual(planning, original)
        self.assertEqual(attr_dict(plan_span['attributes'])['trace_export.snowflake.row']['TRACE'], original['TRACE'])
        for name in ('Plan', 'ReasoningAgentStepPlanning', 'Other'):
            planning['RECORD']['name'] = name
            self.assertEqual(span_of(convert_row(planning)[0])['spanId'], tool_span['spanId'])

    def test_official_otlp_and_message_schemas(self):
        import jsonschema
        from google.protobuf.json_format import ParseDict
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        for row in self.rows[:4]:
            result, _ = convert_row(row)
            wire = copy.deepcopy(result)
            span = span_of(wire)
            # OTLP JSON uses hex IDs; generic protobuf JSON parsers use base64.
            for key in ('traceId', 'spanId', 'parentSpanId'):
                if key in span:
                    span[key] = base64.b64encode(bytes.fromhex(span[key])).decode()
            request = ParseDict(wire, ExportTraceServiceRequest())
            self.assertEqual(len(request.resource_spans[0].scope_spans[0].spans[0].trace_id), 16)
            attrs = attr_dict(span_of(result)['attributes'])
            for direction in ('input', 'output'):
                key = f'gen_ai.{direction}.messages'
                if key in attrs:
                    schema = json.loads((FIXTURES / 'semconv-v1.39.0' / f'gen-ai-{direction}-messages.json').read_text())
                    jsonschema.validate(attrs[key], schema)

    def test_cache_totals_require_explicit_semantics(self):
        result, issues = convert_row(self.rows[1])
        self.assertNotIn('gen_ai.usage.input_tokens', attr_dict(span_of(result)['attributes']))
        self.assertIn('input_token_cache_semantics_unknown', issues)
        for mode, expected in [('includes-cache', 100), ('excludes-cache', 140)]:
            attrs = attr_dict(span_of(convert_row(self.rows[1], mode)[0])['attributes'])
            self.assertEqual(attrs['gen_ai.usage.input_tokens'], expected)
            self.assertEqual(attrs['gen_ai.usage.output_tokens'], 20)

    def test_messages_tool_arguments_and_raw_attributes(self):
        result, issues = convert_row(self.rows[0])
        attrs = attr_dict(span_of(result)['attributes'])
        self.assertEqual(attrs['gen_ai.output.messages'][0]['finish_reason'], 'unknown')
        self.assertIn('output_finish_reason_unavailable', issues)
        self.assertEqual(attrs[PREFIX+'object.name'], 'TEST_AGENT')
        self.assertIsNone(attrs['trace_export.snowflake.row']['VALUE'])
        attrs = attr_dict(span_of(convert_row(self.rows[2])[0])['attributes'])
        self.assertEqual(attrs['gen_ai.tool.call.arguments'], {'city': 'Sofia'})

    def test_unmapped_content_stays_raw(self):
        self.rows[1]['RECORD_ATTRIBUTES'][PREFIX+'agent.planning.messages'] = {'unexpected': 'shape'}
        result, issues = convert_row(self.rows[1])
        attrs = attr_dict(span_of(result)['attributes'])
        self.assertNotIn('gen_ai.input.messages', attrs)
        self.assertEqual(attrs[PREFIX+'agent.planning.messages'], {'unexpected': 'shape'})
        self.assertTrue(any(x.startswith('payload_unmapped:') for x in issues))

    def test_bad_cache_count_does_not_produce_partial_total(self):
        self.rows[1]['RECORD_ATTRIBUTES'][PREFIX+'agent.planning.token_count.cache_read_input'] = 'invalid'
        result, issues = convert_row(self.rows[1], 'excludes-cache')
        self.assertNotIn('gen_ai.usage.input_tokens', attr_dict(span_of(result)['attributes']))
        self.assertIn('invalid_token_count:cache_read_input', issues)

    def test_error_status_is_preserved(self):
        self.rows[2]['RECORD']['status'] = {'code': 'STATUS_CODE_ERROR', 'message': 'failed'}
        span = span_of(convert_row(self.rows[2])[0])
        self.assertEqual(span['status'], {'code': 2, 'message': 'failed'})
        self.assertEqual(attr_dict(span['attributes'])['error.type'], '_OTHER')

    def test_bad_ids_and_timestamps_fail(self):
        for key, value in [('START_TIMESTAMP', None), ('TIMESTAMP', 'not-a-timestamp')]:
            row = copy.deepcopy(self.rows[0]); row[key] = value
            with self.assertRaises(ValueError):
                convert_row(row)
        self.rows[0]['TRACE']['trace_id'] = 'not-an-id'
        with self.assertRaises(ValueError):
            convert_row(self.rows[0])

    def test_reversed_timing_preserves_span_parent_and_original_timestamps(self):
        row = self.rows[3]
        row['TIMESTAMP'] = '2020-01-01 00:00:00'
        span = span_of(convert_row(row)[0])
        attrs = attr_dict(span['attributes'])
        self.assertEqual(span['endTimeUnixNano'], span['startTimeUnixNano'])
        self.assertEqual(span['parentSpanId'], row['RECORD']['parent_span_id'])
        self.assertEqual(span['spanId'], row['TRACE']['span_id'])
        self.assertTrue(attrs['trace_export.timing.corrected'])
        self.assertEqual(attrs['trace_export.timing.reason'], 'end_precedes_start')
        self.assertEqual(attrs['trace_export.snowflake.row']['TIMESTAMP'], row['TIMESTAMP'])
        self.assertEqual(attrs['trace_export.snowflake.row']['START_TIMESTAMP'], row['START_TIMESTAMP'])

    def test_role_prefixed_messages_preserve_order_and_embedded_prefixes(self):
        value = json.dumps(['User: first\nAssistant: quoted text', 'Assistant: answer', 'User: next'])
        result = messages(value, 'user', False, [])
        self.assertEqual([m['role'] for m in result], ['user', 'assistant', 'user'])
        self.assertEqual(result[0]['parts'][0]['content'], 'first\nAssistant: quoted text')
        with self.assertRaises(ValueError):
            messages(json.dumps(['unlabeled']), 'user', False, [])

    def test_json_response_text_is_not_interpreted_as_multiple_messages(self):
        for value in ['["one", "two"]', '123', '{"items": [1, 2]}', '[]']:
            result = messages(value, 'assistant', True, [])
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['parts'][0]['content'], value)

    def test_auto_input_total_requires_consistent_total_and_cache_counts(self):
        attrs = self.rows[1]['RECORD_ATTRIBUTES']
        key = PREFIX+'agent.planning.token_count.'
        for total, cache, expected in [(120,40,100),(160,40,None),(120,101,None)]:
            attrs[key+'total'], attrs[key+'cache_read_input'] = total, cache
            result, _ = convert_row(self.rows[1])
            normalized = attr_dict(span_of(result)['attributes'])
            self.assertEqual(normalized.get('gen_ai.usage.input_tokens'), expected)

    def test_timezone_and_nanosecond_precision(self):
        self.assertEqual(unix_nano('2026-09-24T03:00:00.123456789+03:00'), unix_nano('2026-09-24 00:00:00.123456789'))
        self.assertEqual(unix_nano('1970-01-01T00:00:00.000000001Z'), '1')

    def test_non_span_is_skipped(self):
        self.assertEqual(convert_row(self.rows[-1]), (None, []))

    def test_cli_atomic_failure_and_explicit_skip(self):
        with tempfile.TemporaryDirectory() as folder, redirect_stderr(io.StringIO()):
            source, output = Path(folder)/'raw.jsonl', Path(folder)/'otel.jsonl'
            source.write_text(json.dumps(self.rows[0])+'\ninvalid\n'+json.dumps(self.rows[-1])+'\n')
            output.write_text('original')
            args = ['--input', str(source), '--output', str(output), '--overwrite']
            self.assertEqual(main(args), 1)
            self.assertEqual(output.read_text(), 'original')
            self.assertEqual(main(args+['--skip-invalid']), 0)
            self.assertEqual(len(output.read_text().splitlines()), 1)
            self.assertEqual(len(list(Path(folder).iterdir())), 2)

    def test_export_removes_temporary_raw_and_retains_only_when_requested(self):
        spec = importlib.util.spec_from_file_location('export_under_test', Path(__file__).parents[1]/'export_traces.py')
        exporter = importlib.util.module_from_spec(spec); spec.loader.exec_module(exporter)
        connector = types.ModuleType('snowflake.connector')
        snowflake = types.ModuleType('snowflake'); snowflake.connector = connector
        connector.connect = MagicMock()
        cursor = connector.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        original_write = exporter.write_rows
        paths = []
        def record_write(cur, path, *args):
            paths.append(path)
            return original_write(cur, path, *args)
        with tempfile.TemporaryDirectory() as folder, patch.dict(sys.modules, {'snowflake': snowflake, 'snowflake.connector': connector}), patch.object(exporter, 'write_rows', side_effect=record_write), redirect_stderr(io.StringIO()):
            output, raw = Path(folder)/'otel.jsonl', Path(folder)/'raw.jsonl'
            args = ['--database','DB','--schema','SC','--agent','AG','--output',str(output)]
            cursor.fetchmany.side_effect = [[(json.dumps(self.rows[0]),)], []]
            self.assertEqual(exporter.main(args), 0)
            self.assertFalse(paths[-1].exists())
            cursor.fetchmany.side_effect = [[(json.dumps(self.rows[0]),)], []]
            self.assertEqual(exporter.main(args+['--overwrite','--keep-raw',str(raw)]), 0)
            self.assertTrue(raw.exists())
            bad = copy.deepcopy(self.rows[0]); bad['TRACE']['span_id']='invalid'
            before = output.read_text()
            cursor.fetchmany.side_effect = [[(json.dumps(bad),)], []]
            self.assertEqual(exporter.main(args+['--overwrite']), 1)
            self.assertFalse(paths[-1].exists())
            self.assertEqual(output.read_text(), before)


if __name__ == '__main__':
    unittest.main()
