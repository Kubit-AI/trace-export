from contextlib import ExitStack, redirect_stdout, redirect_stderr
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import MagicMock, patch

from trace_export.export_options import resolve_options

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 29, 12, tzinfo=timezone.utc)


def exporter_for(source):
    spec = importlib.util.spec_from_file_location(source + '_sample_export', ROOT/source/'export_traces.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def selection(source):
    # ClickHouse needs no selection: it defaults to events_full across all readable projects.
    return {'snowflake': ['--agent', 'DB.SC.AG'], 'databricks': ['--table', 'main.demo.agent_trace_unified'],
            'clickhouse': []}[source]


class SamplingTests(TestCase):
    def test_defaults_and_explicit_bounds(self):
        for source in ('snowflake', 'databricks', 'clickhouse'):
            cli = exporter_for(source).parser()
            cases = [([], NOW-timedelta(days=1), NOW, 10),
                     (['--limit','25'], NOW-timedelta(days=1), NOW, 25),
                     (['--until','2020-01-02T00:00:00Z'], datetime(2020,1,1,tzinfo=timezone.utc), datetime(2020,1,2,tzinfo=timezone.utc), 10),
                     (['--since','2020-01-01T00:00:00Z'], datetime(2020,1,1,tzinfo=timezone.utc), NOW, 10),
                     (['--since','2020-01-01T00:00:00Z','--until','2020-02-01T00:00:00Z'], datetime(2020,1,1,tzinfo=timezone.utc), datetime(2020,2,1,tzinfo=timezone.utc),10),
                     (['--all-time'], None, None, 10),
                     (['--no-sample','--all-time','--output','out.jsonl'], None, None, None),
                     (['--no-sample','--all-time','--output','out.jsonl','--limit','20'], None,None,20)]
            for options, since, until, limit in cases:
                with self.subTest(source=source, options=options):
                    args = cli.parse_args(selection(source)+options)
                    resolve_options(args, cli, NOW)
                    self.assertEqual((args.since,args.until,args.limit), (since,until,limit))

    def test_invalid_combinations_fail_before_connecting(self):
        for source in ('snowflake','databricks','clickhouse'):
            cli = exporter_for(source).parser()
            for options in (['--no-sample'], ['--no-sample','--output','out.jsonl'],
                            ['--no-sample','--all-time'], ['--json-summary'], ['--overwrite'],
                            ['--all-time','--since','2020-01-01T00:00:00Z'],
                            ['--since','2030-01-01T00:00:00Z'], ['--limit','0'],
                            ['--query-timeout','0'], ['--query-timeout','172801']):
                with self.subTest(source=source,options=options), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
                    args = cli.parse_args(selection(source)+options)
                    resolve_options(args,cli,NOW)
                self.assertEqual(exc.exception.code,2)

    def test_stdout_is_completed_jsonl_and_timeout_precedes_query(self):
        for source in ('snowflake','databricks'):
            exporter = exporter_for(source)
            fixture = ROOT/source/'tests/fixtures'/('cortex_rows.json' if source == 'snowflake' else 'unified_row.json')
            row = json.loads(fixture.read_text())
            if source == 'snowflake': row = row[0]
            for fmt in ('otel','raw'):
                with self.subTest(source=source,format=fmt), ExitStack() as stack:
                    connection = MagicMock()
                    cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
                    if source == 'snowflake':
                        stack.enter_context(patch('snowflake.connector.connect', return_value=connection))
                        method = 'write_rows'
                    else:
                        stack.enter_context(patch.object(exporter,'settings',return_value={}))
                        stack.enter_context(patch.object(exporter,'connect',return_value=connection))
                        method = 'download'
                    downloaded_paths = []
                    original = getattr(exporter,method)
                    def download(cur,path,*args):
                        downloaded_paths.append(path)
                        return original(cur,path,*args)
                    stack.enter_context(patch.object(exporter,method,side_effect=download))
                    stack.enter_context(redirect_stderr(io.StringIO()))
                    cursor.fetchmany.side_effect = [[(json.dumps(row),)],[]]
                    args = selection(source)+['--format',fmt,'--query-timeout','42']
                    with redirect_stdout(io.StringIO()) as out:
                        self.assertEqual(exporter.main(args),0)
                    payload = json.loads(out.getvalue())
                    self.assertIn('resourceSpans' if fmt == 'otel' else ('TRACE' if source == 'snowflake' else 'trace_id'),payload)
                    self.assertFalse(downloaded_paths[0].exists())
                    statements = [call.args[0] for call in cursor.execute.call_args_list]
                    self.assertIn('TIMEOUT',statements[0])
                    self.assertIn('42',statements[0])
                    self.assertNotIn('ORDER BY',statements[-1])
                    self.assertEqual(cursor.execute.call_args.args[1][-1],10)
                    self.assertIn('LIMIT',statements[-1])
                    if fmt == 'otel':
                        # A valid first row followed by bad data must publish nothing.
                        cursor.fetchmany.side_effect = [[(json.dumps(row),),('{}',)],[]]
                        with redirect_stdout(io.StringIO()) as out:
                            self.assertEqual(exporter.main(args),1)
                        self.assertEqual(out.getvalue(),'')
                        self.assertFalse(downloaded_paths[-1].exists())
                    cursor.execute.reset_mock()
                    cursor.execute.side_effect = RuntimeError('timeout setting rejected')
                    with redirect_stdout(io.StringIO()) as out:
                        self.assertEqual(exporter.main(args),1)
                    self.assertEqual(out.getvalue(),'')
                    self.assertEqual(cursor.execute.call_count,1)
