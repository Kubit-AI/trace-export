import importlib.util
import io
import json
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


class ExportSummaryTests(TestCase):
    def test_success_and_failed_conversion_are_machine_readable(self):
        for source in ('snowflake', 'databricks'):
            spec = importlib.util.spec_from_file_location(source+'_summary_export', ROOT/source/'export_traces.py')
            exporter = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(exporter)
            if source == 'snowflake':
                row = json.loads((ROOT/'snowflake/tests/fixtures/cortex_rows.json').read_text())[0]
                selection = ['--agent','DB.SC.AG']
            else:
                row = json.loads((ROOT/'databricks/tests/fixtures/unified_row.json').read_text())
                selection = ['--table','main.demo.agent_trace_unified']
            for output_format in ('raw', 'otel'):
                with self.subTest(source=source, format=output_format), ExitStack() as stack:
                    folder = stack.enter_context(tempfile.TemporaryDirectory())
                    connection = MagicMock()
                    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
                    if source == 'snowflake':
                        stack.enter_context(patch('snowflake.connector.connect', return_value=connection))
                    else:
                        stack.enter_context(patch.object(exporter, 'connect', return_value=connection))
                        stack.enter_context(patch.object(exporter, 'settings', return_value={}))
                    stack.enter_context(redirect_stderr(io.StringIO()))
                    output = Path(folder)/'out.jsonl'
                    args = selection + ['--output', str(output), '--format', output_format, '--json-summary', '--limit', '1']
                    cursor.fetchmany.side_effect = [[(json.dumps(row),)], []]
                    with redirect_stdout(io.StringIO()) as out:
                        self.assertEqual(exporter.main(args), 0)
                    summary = json.loads(out.getvalue())
                    self.assertEqual(summary['downloaded_rows'], 1)
                    self.assertTrue(summary['output_written'])
                    self.assertEqual(summary['format'], output_format)
                    self.assertTrue(summary['possibly_truncated'])
                    self.assertTrue(summary['sample'])
                    self.assertEqual(summary['query_timeout_seconds'], 300)
                    if output_format == 'otel':
                        original = output.read_text()
                        cursor.fetchmany.side_effect = [[('{}',)], []]
                        with redirect_stdout(io.StringIO()) as out:
                            self.assertEqual(exporter.main(args+['--overwrite']), 1)
                        self.assertFalse(json.loads(out.getvalue())['output_written'])
                        self.assertEqual(output.read_text(), original)
