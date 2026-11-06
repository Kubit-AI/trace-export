import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "export_traces", Path(__file__).resolve().parents[1] / "export_traces.py"
)
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)


class Cursor:
    def __init__(self, batches):
        self.batches = iter(batches)

    def fetchmany(self, size):
        batch = next(self.batches, [])
        if isinstance(batch, Exception):
            raise batch
        return batch


class ExportTests(unittest.TestCase):
    def test_preserves_database_rows_and_numeric_precision(self):
        payload = '{\n"RECORD":{"text":"hello\\nworld","items":[1,null]},"VALUE":null,"N":0.12345678901234567890123456789\n}'
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "rows.jsonl"
            count = exporter.write_rows(Cursor([[(payload,)], [('{}',)]]), output)
            lines = output.read_text().splitlines()
            self.assertEqual(count, 2)
            self.assertEqual(len(lines), 2)
            self.assertIn('0.12345678901234567890123456789', lines[0])
            row = json.loads(lines[0])
            self.assertIsNone(row['VALUE'])
            self.assertEqual(row['RECORD']['text'], 'hello\nworld')

    def test_failed_download_preserves_existing_file_and_cleans_partial(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "rows.jsonl"
            output.write_text('original\n')
            cursor = Cursor([[('{}',)], RuntimeError('connection lost')])
            with self.assertRaises(RuntimeError):
                exporter.write_rows(cursor, output, overwrite=True)
            self.assertEqual(output.read_text(), 'original\n')
            self.assertEqual(list(Path(folder).iterdir()), [output])

    def test_existing_file_requires_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "rows.jsonl"
            output.write_text('original\n')
            with self.assertRaises(FileExistsError):
                exporter.write_rows(Cursor([]), output)
            self.assertEqual(output.read_text(), 'original\n')
            self.assertEqual(exporter.write_rows(Cursor([]), output, overwrite=True), 0)
            self.assertEqual(output.read_text(), '')

    def test_invalid_payload_is_not_published(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "rows.jsonl"
            with self.assertRaises(ValueError):
                exporter.write_rows(Cursor([[('not json',)]]), output)
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_names_are_bound_and_time_bounds_are_utc(self):
        args = exporter.parser().parse_args([
            '--database', 'DB', '--schema', 'SCHEMA', '--agent', "agent'quoted",
            '--since', '2026-09-24T03:00:00+03:00',
            '--until', '2026-09-25T00:00:00Z', '--limit', '100',
            '--output', 'unused.jsonl',
        ])
        sql, params = exporter.build_query(args)
        self.assertNotIn(args.agent, sql)
        self.assertEqual(params, ('DB', 'SCHEMA', "agent'quoted",
                                 '2026-09-24T00:00:00+00:00',
                                 '2026-09-25T00:00:00+00:00', 100))
        self.assertIn('>= TO_TIMESTAMP_TZ(%s)', sql)
        self.assertIn('< TO_TIMESTAMP_TZ(%s)', sql)
        self.assertIn("e.RECORD_TYPE = 'SPAN'", sql)
        self.assertIn('e."START_TIMESTAMP" >=', sql)
        self.assertNotIn('ORDER BY', sql)
        args.order_by_time = True
        self.assertIn('ORDER BY e."START_TIMESTAMP"', exporter.build_query(args)[0])

    def test_all_events_and_event_time_are_explicit_options(self):
        args = exporter.parser().parse_args([
            '--database', 'DB', '--schema', 'SCHEMA', '--agent', 'AGENT',
            '--all-events', '--time-column', 'TIMESTAMP',
            '--since', '2026-09-24T00:00:00Z', '--output', 'unused.jsonl',
        ])
        sql, _ = exporter.build_query(args)
        self.assertNotIn("RECORD_TYPE = 'SPAN'", sql)
        self.assertIn('e."TIMESTAMP" >=', sql)
        self.assertNotIn('START_TIMESTAMP', sql)

    def test_qualified_agent_binds_three_names(self):
        args = exporter.parser().parse_args([
            '--agent', 'EXAMPLE_DB.AGENTS.EXAMPLE_AGENT',
            '--output', 'unused.jsonl',
        ])
        sql, params = exporter.build_query(args)
        self.assertEqual(params, ('EXAMPLE_DB', 'AGENTS', 'EXAMPLE_AGENT'))
        self.assertNotIn('EXAMPLE_DB', sql)

    def test_invalid_or_partial_agent_qualification(self):
        for options in [[], ['--database', 'DB'], ['--schema', 'SCHEMA']]:
            args = exporter.parser().parse_args(['--agent', 'AGENT', '--output', 'unused.jsonl'] + options)
            with self.assertRaises(ValueError):
                exporter.agent_parts(args)
        for agent in ['DB..AGENT', 'DB.SCHEMA.AGENT.EXTRA']:
            args = exporter.parser().parse_args(['--agent', agent, '--output', 'unused.jsonl'])
            with self.assertRaises(ValueError):
                exporter.agent_parts(args)

    def test_separate_options_preserve_literal_dots(self):
        args = exporter.parser().parse_args([
            '--database', 'my.db', '--schema', 'my.schema', '--agent', 'my.agent',
            '--output', 'unused.jsonl',
        ])
        self.assertEqual(exporter.agent_parts(args), ('my.db', 'my.schema', 'my.agent'))


if __name__ == '__main__':
    unittest.main()
