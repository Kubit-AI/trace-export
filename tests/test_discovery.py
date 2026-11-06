import io
import json
from contextlib import redirect_stdout, redirect_stderr
from unittest import TestCase
from unittest.mock import patch, MagicMock
from trace_export.discovery import databricks_discover, snowflake_discover, main


class Cursor:
    def __init__(self, responses):
        self.responses = responses
        self.queries = []

    def execute(self, sql):
        self.queries.append(sql)
        value = self.responses[sql]
        if isinstance(value, Exception):
            raise value
        columns, self.pending = value
        self.description = [(name,) for name in columns]

    def fetchmany(self, size):
        batch, self.pending = self.pending[:size], self.pending[size:]
        return batch


class DiscoveryTests(TestCase):
    def test_snowflake_scope_quoting_and_filter(self):
        cursor = Cursor({'SHOW AGENTS IN SCHEMA "DB""X"."S"': (
            ['name','database_name','schema_name'], [('Demo','DB"X','S'), ('Other','DB"X','S')])})
        result = snowflake_discover(cursor, 'DB"X', 'S', 'dem*')
        self.assertEqual(len(result['items']), 1)
        self.assertEqual(result['items'][0]['export_args'], ['--database','DB"X','--schema','S','--agent','Demo'])
        self.assertTrue(result['complete'])
        self.assertIn('hidden by permissions', result['visibility_note'])

    def test_snowflake_truncation_is_not_complete(self):
        cursor = Cursor({'SHOW AGENTS IN ACCOUNT': (['name','database_name','schema_name'], [('a','d','s')]*10000)})
        result = snowflake_discover(cursor, match='no-match')
        self.assertEqual(result['status'], 'partial')
        self.assertFalse(result['complete'])

    def test_databricks_scans_and_preserves_partial_matches(self):
        cursor = Cursor({
            'SHOW CATALOGS': (['catalog'], [('main',), ('private',)]),
            'SHOW SCHEMAS IN `main`': (['databaseName'], [('demo',), ('blocked',)]),
            'SHOW SCHEMAS IN `private`': PermissionError('do not expose secret'),
            'SHOW TABLES IN `main`.`blocked`': PermissionError('denied'),
            'SHOW TABLES IN `main`.`demo`': (['database','tableName','isTemporary'], [
                ('demo','a_trace_unified',False), ('demo','bad_trace_unified',False),
                ('demo','temp_trace_unified',True), ('demo','unrelated',False)]),
            'DESCRIBE TABLE `main`.`demo`.`a_trace_unified`': (['col_name'], [('trace_id',),('spans',),('request_time',)]),
            'DESCRIBE TABLE `main`.`demo`.`bad_trace_unified`': (['col_name'], [('trace_id',)]),
        })
        result = databricks_discover(cursor)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(len(result['issues']), 2)
        self.assertNotIn('secret', json.dumps(result))
        good, bad = result['items']
        self.assertEqual(good['validation'], 'columns_present')
        self.assertEqual(bad['missing_columns'], ['request_time','spans'])
        self.assertIsNone(bad['export_args'])
        self.assertFalse(any('SELECT' in sql for sql in cursor.queries))

    def test_candidate_does_not_claim_validation(self):
        cursor = Cursor({'SHOW TABLES IN `main`.`demo`': (['tableName'], [('a_trace_unified',)])})
        result = databricks_discover(cursor, 'main', 'demo', verify=False)
        self.assertEqual(result['items'][0]['validation'], 'candidate')
        self.assertTrue(result['complete'])

    def test_failed_column_check_keeps_candidate(self):
        cursor = Cursor({
            'SHOW TABLES IN `main`.`demo`': (['tableName'], [('a_trace_unified',)]),
            'DESCRIBE TABLE `main`.`demo`.`a_trace_unified`': PermissionError(),
        })
        result = databricks_discover(cursor, 'main', 'demo')
        self.assertEqual(result['items'][0]['validation'], 'unverified')
        self.assertEqual(result['status'], 'partial')

    def test_human_default_json_opt_in_and_visibility_message(self):
        connection = MagicMock()
        cursor = Cursor({'SHOW AGENTS IN ACCOUNT': (['name','database_name','schema_name'], [])})
        connection.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        with patch('snowflake.connector.connect', return_value=connection):
            human = io.StringIO()
            with redirect_stdout(human):
                self.assertEqual(main('snowflake', ['--connection','traces']), 0)
            self.assertIn('may be hidden by permissions', human.getvalue())
            self.assertIn('MONITOR (or OWNERSHIP)', human.getvalue())
            self.assertIn('its absence does not itself hide agents', human.getvalue())
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(main('snowflake', ['--connection','traces','--json']), 0)
            result = json.loads(output.getvalue())
            self.assertEqual(result['items'], [])
            self.assertIn('permissions', result['empty_result_note'])
            self.assertIn('SNOWFLAKE.CORTEX_USER', ' '.join(result['permission_guidance']))
            self.assertIn('READ UNREDACTED AI OBSERVABILITY EVENTS TABLE ON ACCOUNT',
                          ' '.join(result['permission_guidance']))

    def test_failure_is_not_a_successful_empty_search(self):
        with patch('snowflake.connector.connect', side_effect=RuntimeError('secret credential')), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main('snowflake', ['--json']), 1)
        result = json.loads(out.getvalue())
        self.assertEqual(result['status'], 'error')
        self.assertFalse(result['complete'])
        self.assertNotIn('secret credential', out.getvalue())

    def test_schema_requires_parent(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
            main('databricks', ['--schema','demo'])
        self.assertEqual(exc.exception.code, 2)

    def test_source_options_stay_separate(self):
        for source, options in (('snowflake', ['--url', 'http://h']), ('snowflake', ['--env-file', 'x']),
                                ('databricks', ['--url', 'http://h']), ('databricks', ['--database', 'd'])):
            with self.subTest(source=source, options=options), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as exc:
                main(source, options)
            self.assertEqual(exc.exception.code, 2)
