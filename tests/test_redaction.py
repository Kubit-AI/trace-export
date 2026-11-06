from contextlib import redirect_stdout, redirect_stderr
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch
from trace_export.redaction import CONTENT_KEYS, PREFIX, export_warnings, row_has_content

ROOT = Path(__file__).resolve().parents[1]


class RedactionTests(TestCase):
    def test_absent_null_empty_and_message_scaffolding(self):
        for value in (None, '', '  ', [], {}, 'null', '[]', '{}', '""', {'nested':[None,'']},
                      [{'role':'user','content':[]}], '[{"role":"assistant","content":""}]'):
            with self.subTest(value=value):
                row = {'RECORD_ATTRIBUTES': {PREFIX+'messages':value, PREFIX+'planning.model':'model',
                                             PREFIX+'planning.token_count.input':20, PREFIX+'tool.id':'tool'}}
                self.assertFalse(row_has_content(row))

    def test_each_content_category_suppresses_warning(self):
        for key in CONTENT_KEYS | {PREFIX+'tool.code_execution.argument.value',
                                   PREFIX+'tool.custom.output', PREFIX+'feedback.comment'}:
            with self.subTest(key=key):
                self.assertTrue(row_has_content({'RECORD_ATTRIBUTES':{key:'actual content'}}))
        self.assertTrue(row_has_content({'RECORD_ATTRIBUTES':{PREFIX+'tool.custom.output':False}}))
        self.assertTrue(row_has_content({'RECORD_ATTRIBUTES':{PREFIX+'feedback.score':0}}))
        self.assertTrue(row_has_content({'RECORD':{'name':'user_feedback'}, 'VALUE':{'comment':'helpful'}}))

    def test_whole_export_and_zero_rows(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'rows.jsonl'
            path.write_text('{}\n{}\n')
            self.assertEqual(export_warnings(path,0),[])
            self.assertEqual(len(export_warnings(path,2)),1)
            with path.open('a') as handle:
                handle.write(json.dumps({'RECORD_ATTRIBUTES':{PREFIX+'response':'visible'}})+'\n')
            self.assertEqual(export_warnings(path,3),[])

    def test_raw_and_otel_warning_stderr_and_summary_data_unchanged(self):
        spec=importlib.util.spec_from_file_location('snowflake_redaction_export',ROOT/'snowflake/export_traces.py')
        exporter=importlib.util.module_from_spec(spec);spec.loader.exec_module(exporter)
        row=json.loads((ROOT/'snowflake/tests/fixtures/cortex_rows.json').read_text())[0]
        row['RECORD_ATTRIBUTES']={}
        for fmt in ('raw','otel'):
            with self.subTest(format=fmt), tempfile.TemporaryDirectory() as folder:
                output=Path(folder)/'out.jsonl'
                connection=MagicMock()
                cursor=connection.__enter__.return_value.cursor.return_value.__enter__.return_value
                cursor.fetchmany.side_effect=[[(json.dumps(row),)],[]]
                with patch('snowflake.connector.connect',return_value=connection), redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()) as err:
                    result=exporter.main(['--agent','DB.SC.AG','--format',fmt,'--output',str(output),'--json-summary'])
                self.assertEqual(result,0)
                summary=json.loads(out.getvalue())
                self.assertEqual(summary['warnings'][0]['code'],'snowflake_payloads_empty_possible_redaction')
                self.assertEqual(err.getvalue().count('Warning [snowflake_payloads_empty_possible_redaction]'),1)
                self.assertIn('genuinely empty',err.getvalue())
                if fmt=='raw':self.assertEqual(json.loads(output.read_text()),row)
                else:self.assertIn('resourceSpans',json.loads(output.read_text()))
                data_queries=[c for c in cursor.execute.call_args_list if c.args[0].startswith('SELECT')]
                self.assertEqual(len(data_queries),1)
