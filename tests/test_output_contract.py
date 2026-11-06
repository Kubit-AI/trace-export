import copy
import io
import json
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
import tempfile
from unittest import TestCase
from trace_export.cortex import convert_row as cortex
from trace_export.mlflow import convert_row as mlflow
from trace_export.reader import iter_spans, decode_value
from trace_export.validate import validate_record, validator, main

ROOT = Path(__file__).resolve().parents[1]


class ContractTests(TestCase):
    def setUp(self):
        self.check = validator()
        self.record, _ = mlflow(json.loads((ROOT/'databricks/tests/fixtures/unified_row.json').read_text()))

    def test_real_converters_match_published_schema(self):
        self.assertEqual(validate_record(self.record, self.check), 4)
        for row in json.loads((ROOT/'snowflake/tests/fixtures/cortex_rows.json').read_text()):
            record, _ = cortex(row)
            if record:
                self.assertEqual(validate_record(record, self.check), 1)

    def test_examples_and_reader_context(self):
        examples = sorted(path.name for path in (ROOT/'examples').glob('*.jsonl'))
        self.assertEqual(examples, ['clickhouse.otel.jsonl', 'databricks.otel.jsonl', 'logfire.otel.jsonl', 'snowflake.otel.jsonl'])
        for path in (ROOT/'examples').glob('*.jsonl'):
            for line in path.read_text().splitlines():
                validate_record(json.loads(line), self.check)
        clickhouse = list(iter_spans(ROOT/'examples/clickhouse.otel.jsonl'))
        self.assertEqual({item.resource_attributes['langfuse.project.id'] for item in clickhouse}, {'example-project'})
        self.assertEqual(len({item.span['traceId'] for item in clickhouse}), 1)
        for item in clickhouse:
            self.assertNotIn('ingestion_api_key', item.attributes['trace_export.langfuse.row'])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'data.jsonl'
            path.write_text(json.dumps(self.record)+'\n'+json.dumps({'resourceSpans':[]})+'\n')
            items = list(iter_spans(path))
        self.assertEqual(len(items), 4)
        self.assertEqual(items[0].line_number, 1)
        self.assertEqual(items[0].attributes['gen_ai.operation.name'], 'invoke_agent')
        self.assertIn('trace_export.databricks.row', items[0].scope_attributes)
        self.assertIsInstance(items[0].resource_attributes, dict)
        self.assertEqual(decode_value({'intValue':'9223372036854775807'}), 2**63-1)
        self.assertIsNone(decode_value({}))

    def test_snowflake_example_matches_fixture_conversion(self):
        rows = json.loads((ROOT/'snowflake/tests/fixtures/cortex_rows.json').read_text())
        expected = [record for record, _ in map(cortex, rows) if record]
        published = [json.loads(line) for line in (ROOT/'examples/snowflake.otel.jsonl').read_text().splitlines()]
        self.assertEqual(published, expected)

    def test_rejects_bad_ids_ranges_timing_and_attribute_union(self):
        for key, value in [('traceId','0'*32), ('spanId','xyz'), ('startTimeUnixNano',str(2**64)),
                           ('startTimeUnixNano',-1), ('kind',6), ('flags',2**32),
                           ('attributes',[{'key':'x','value':{'stringValue':'x','intValue':'1'}}]),
                           ('attributes',[{'key':'x','value':{'intValue':str(2**63)}}]),
                           ('attributes',[{'key':'x','value':{'intValue':str(-2**63-1)}}]),
                           ('endTimeUnixNano','0')]:
            record = copy.deepcopy(self.record)
            record['resourceSpans'][0]['scopeSpans'][0]['spans'][0][key] = value
            with self.subTest(key=key,value=value), self.assertRaises(ValueError):
                validate_record(record, self.check)

    def test_suffixed_span_ids_and_strict_trace_ids(self):
        from trace_export.otlp import identifier
        self.assertEqual(identifier('ABCDEF0123456789-Plan', 16, 'span ID'), 'abcdef0123456789-Plan')
        for value in ('0'*16 + '-1', 'abcdef012345678', 'g' + '1'*15):
            with self.subTest(value=value), self.assertRaises(ValueError):
                identifier(value, 16, 'span ID')
        with self.assertRaises(ValueError):
            identifier('1'*32 + '-1', 32, 'trace ID')
        span = self.record['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        span['spanId'] += '-1'
        span['parentSpanId'] = 'abcdef0123456789-parent'
        validate_record(self.record, self.check)
        for key, value in [('spanId', '0'*16 + '-1'), ('parentSpanId', 'short'),
                           ('traceId', '1'*32 + '-1')]:
            record = copy.deepcopy(self.record)
            record['resourceSpans'][0]['scopeSpans'][0]['spans'][0][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_record(record, self.check)
        row = json.loads((ROOT/'snowflake/tests/fixtures/cortex_rows.json').read_text())[1]
        row['RECORD']['name'] = 'ReasoningAgentStepPlanning-0'
        validate_record(cortex(row)[0], self.check)

    def test_valid_numeric_boundaries(self):
        record = copy.deepcopy(self.record)
        span = record['resourceSpans'][0]['scopeSpans'][0]['spans'][0]
        span['startTimeUnixNano'] = '0'
        span['endTimeUnixNano'] = str(2**64-1)
        span['attributes'] = [{'key':str(i), 'value':{'intValue':str(i)}} for i in (0,-1,2**63-1,-2**63)]
        validate_record(record, self.check)

    def test_validator_line_numbers_and_no_payload_leak(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'data.jsonl'
            path.write_text(json.dumps(self.record)+'\n{"secret":"payload"}\n')
            with redirect_stdout(io.StringIO()) as out:
                self.assertEqual(main([str(path),'--json']), 1)
            result = json.loads(out.getvalue())
            self.assertEqual(result['rows_validated'], 1)
            self.assertIn('line 2', result['error'])
            self.assertNotIn('payload', result['error'])
            path.write_text('{"resourceSpans":[],"secret": NaN}\n')
            with self.assertRaises(ValueError):
                list(iter_spans(path))
