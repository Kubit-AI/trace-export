"""ClickHouse query and download tests use synthetic rows and fake clients; they never contact a server."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from trace_export.clickhouse_source import build_query, download, export_columns, query_settings

FIXTURE = json.loads((Path(__file__).with_name("fixtures") / "events_full_schema.json").read_text())
SCHEMA = {"database": "default", "table": "events_full", "columns": dict(FIXTURE["columns"])}
COLUMNS = export_columns(SCHEMA)
SENTINEL = "SENTINEL-ROW-CONTENT"
PREFLIGHT = ('{"time":"2001-02-03T04:05:06.789012Z","uint":18446744073709551615,"int":-9223372036854775808,'
             '"decimal":"-1.000000000001","uints":{"k":18446744073709551615},"decimals":{"k":"0.000000000001"},'
             '"utf8":"a�","max_execution_time":"300","timeout_overflow_mode":"throw"}\n')


def row(index=0, **values):
    data = {column: "" for column in COLUMNS}
    data.update(span_id=f"span-{index}", start_time="2026-01-02T03:04:05.123456Z", is_deleted=0,
                input=f"{SENTINEL} é𝄞中   \"quoted\"\n", input_length=18446744073709551615,
                usage_details={"input": 18446744073709551615}, cost_details={"total": "0.000000000001"},
                calculated_total_cost="123456.789012345678", **values)
    # Server-shaped JSONEachRow line: compact, UTF-8, unquoted UInt64, quoted decimals.
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"


class ChunkStream:
    def __init__(self, body, chunk):
        self.body, self.chunk, self.position, self.closed = body, chunk, 0, False

    def read(self, size):
        size = min(size, self.chunk)
        data = self.body[self.position:self.position + size]
        self.position += len(data)
        return data

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, body=b"", preflight=PREFLIGHT.encode(), chunk=7):
        self.body, self.preflight, self.chunk, self.calls, self.stream = body, preflight, chunk, [], None

    def raw_query(self, sql, settings=None, fmt=None):
        self.calls.append(("raw_query", sql, None, settings, fmt))
        if isinstance(self.preflight, Exception):
            raise self.preflight
        return self.preflight

    def raw_stream(self, sql, parameters=None, settings=None, fmt=None):
        self.calls.append(("raw_stream", sql, parameters, settings, fmt))
        self.stream = ChunkStream(self.body, self.chunk)
        return self.stream


class QueryTests(unittest.TestCase):
    def test_bounds_and_project_are_bound_parameters(self):
        since = datetime(2026, 1, 2, 5, 4, 5, 123456, tzinfo=timezone(timedelta(hours=2)))
        sql, parameters = build_query(SCHEMA, since=since, until=since + timedelta(microseconds=1),
                                      project_id="p'); DROP TABLE x; --", limit=25)
        self.assertEqual(parameters, {"since": "2026-01-02 03:04:05.123456", "until": "2026-01-02 03:04:05.123457",
                                      "project_id": "p'); DROP TABLE x; --"})
        self.assertIn("FROM `default`.`events_full` FINAL WHERE `is_deleted` = 0", sql)
        self.assertIn("`start_time` >= {since:DateTime64(6, 'UTC')} AND `start_time` < {until:DateTime64(6, 'UTC')}",
                      sql)
        self.assertIn("`project_id` = {project_id:String}", sql)
        self.assertNotIn("DROP", sql)
        self.assertTrue(sql.endswith(" LIMIT 25"))
        self.assertNotIn("ORDER BY", sql)
        self.assertNotIn("OFFSET", sql)

    def test_omitted_project_and_bounds_query_all_projects_and_history(self):
        sql, parameters = build_query(SCHEMA)
        self.assertEqual(parameters, {})
        self.assertTrue(sql.endswith("FINAL WHERE `is_deleted` = 0"))
        self.assertNotIn("project_id` =", sql)
        self.assertNotIn("LIMIT", sql)

    def test_columns_include_materialized_and_exclude_ingestion_key(self):
        sql, _ = build_query(SCHEMA)
        selected = sql[len("SELECT "):sql.index(" FROM ")].split(", ")
        self.assertEqual(selected, [f"`{column}`" for column, _ in FIXTURE["columns"] if column != "ingestion_api_key"])
        for column in ("calculated_total_cost", "total_cost", "input_length", "output_length", "event_ts"):
            self.assertIn(f"`{column}`", selected)
        self.assertNotIn("ingestion_api_key", sql)

    def test_order_and_quoted_identifiers(self):
        sql, _ = build_query({**SCHEMA, "columns": {"a`b": "String", "c\\d": "String"}}, order_by_time=True, limit=1)
        self.assertTrue(sql.startswith(r"SELECT `a\`b`, `c\\d` FROM"))
        self.assertTrue(sql.endswith(" ORDER BY `start_time`, `project_id`, `trace_id`, `span_id` LIMIT 1"))

    def test_invalid_arguments_rejected(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        cases = ({"since": datetime(2026, 1, 1)}, {"since": "2026-01-01T00:00:00Z"}, {"since": now, "until": now},
                 {"limit": 0}, {"limit": True}, {"limit": "5"}, {"project_id": ""}, {"project_id": 5})
        for kwargs in cases:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                build_query(SCHEMA, **kwargs)
        with self.assertRaisesRegex(ValueError, "identifiers"):
            build_query({**SCHEMA, "table": "events_full FINAL"})

    def test_settings_pin_encoding_timeout_and_block_size(self):
        self.assertEqual(query_settings(30, 7), {
            "max_execution_time": 30, "timeout_overflow_mode": "throw", "max_block_size": 7,
            "output_format_json_quote_64bit_integers": 0, "output_format_json_quote_decimals": 1,
            "date_time_output_format": "iso", "output_format_json_validate_utf8": 1,
            "optimize_move_to_prewhere_if_final": 0})
        for kwargs in ({"timeout": 0}, {"batch_size": 0}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                query_settings(**kwargs)


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.output = Path(self.folder.name) / "raw.jsonl"

    def assert_clean(self):
        self.assertEqual(list(Path(self.folder.name).iterdir()), [])

    def test_rows_are_written_verbatim_across_split_utf8_chunks(self):
        body = b"".join(row(index) for index in range(5))
        for chunk in (1, 3, 7, 1 << 20):
            with self.subTest(chunk=chunk):
                client = FakeClient(body, chunk=chunk)
                count = download(client, SCHEMA, self.output, project_id="p1", limit=5, batch_size=2,
                                 timeout=300, overwrite=True)
                self.assertEqual(count, 5)
                self.assertEqual(self.output.read_bytes(), body)
                self.assertTrue(client.stream.closed)
        first = json.loads(self.output.read_text(encoding="utf-8").split("\n")[0], parse_int=str)
        self.assertEqual(first["input_length"], "18446744073709551615")
        self.assertEqual(first["usage_details"], {"input": "18446744073709551615"})
        self.assertEqual((first["calculated_total_cost"], first["cost_details"]["total"]),
                         ("123456.789012345678", "0.000000000001"))
        self.assertEqual(first["start_time"], "2026-01-02T03:04:05.123456Z")
        self.assertEqual(first["input"], f"{SENTINEL} é𝄞中   \"quoted\"\n")

    def test_one_preflight_then_one_streamed_data_query(self):
        client = FakeClient(row(), chunk=4)
        self.assertEqual(download(client, SCHEMA, self.output, project_id="p1", batch_size=1, timeout=300), 1)
        (kind, preflight, _, preflight_settings, fmt), (stream_kind, sql, parameters, settings, stream_fmt) = client.calls
        self.assertEqual((kind, stream_kind, fmt, stream_fmt), ("raw_query", "raw_stream", "JSONEachRow", "JSONEachRow"))
        self.assertNotIn("events_full", preflight)
        self.assertEqual(preflight_settings, settings)
        self.assertEqual((settings["max_block_size"], settings["max_execution_time"]), (1, 300))
        self.assertEqual(parameters, {"project_id": "p1"})
        self.assertNotIn("OFFSET", sql)

    def test_empty_result_publishes_empty_file(self):
        self.assertEqual(download(FakeClient(b""), SCHEMA, self.output), 0)
        self.assertEqual(self.output.read_bytes(), b"")

    def test_unapplied_settings_fail_before_data_query(self):
        cases = {
            "uint": PREFLIGHT.replace('"uint":18446744073709551615', '"uint":"18446744073709551615"'),
            "decimal": PREFLIGHT.replace('"decimal":"-1.000000000001"', '"decimal":-1.000000000001'),
            "time": PREFLIGHT.replace("2001-02-03T04:05:06.789012Z", "2001-02-03 13:05:06.789012"),
            "max_execution_time": PREFLIGHT.replace('"max_execution_time":"300"', '"max_execution_time":"0"'),
            "timeout_overflow_mode": PREFLIGHT.replace('"throw"', '"break"'),
        }
        for setting, response in cases.items():
            with self.subTest(setting=setting):
                client = FakeClient(row(), preflight=response.encode())
                with self.assertRaisesRegex(ValueError, f"did not apply.*{setting}"):
                    download(client, SCHEMA, self.output)
                self.assertEqual([call[0] for call in client.calls], ["raw_query"])
                self.assert_clean()
        client = FakeClient(row(), preflight=b"Code: 164. DB::Exception: Cannot modify setting in readonly mode")
        with self.assertRaisesRegex(ValueError, "unexpected settings check"):
            download(client, SCHEMA, self.output)
        self.assert_clean()

    def test_requested_timeout_is_verified(self):
        with self.assertRaisesRegex(ValueError, "max_execution_time"):
            download(FakeClient(row()), SCHEMA, self.output, timeout=30)
        preflight = PREFLIGHT.replace('"max_execution_time":"300"', '"max_execution_time":"30"').encode()
        self.assertEqual(download(FakeClient(row(), preflight=preflight), SCHEMA, self.output, timeout=30), 1)

    def test_driver_rejection_propagates_without_data_query(self):
        client = FakeClient(row(), preflight=RuntimeError("Setting max_execution_time is readonly"))
        with self.assertRaisesRegex(RuntimeError, "readonly"):
            download(client, SCHEMA, self.output)
        self.assertEqual(len(client.calls), 1)
        self.assert_clean()

    def test_appended_timeout_error_fails_and_cleans_up(self):
        error = (b"\r\n__exception__\r\nqwertyuiopasdfgh\r\nCode: 159. DB::Exception: Timeout exceeded: elapsed 300 "
                 b"seconds, maximum: 300. (TIMEOUT_EXCEEDED)\n120 qwertyuiopasdfgh\r\n__exception__\r\n")
        for body in (row(0) + row(1) + error, row(0) + row(1)[:40] + error):
            with self.subTest(body=len(body)):
                client = FakeClient(body, chunk=5)
                with self.assertRaisesRegex(RuntimeError, "after streaming started: Code: 159.*TIMEOUT_EXCEEDED") as caught:
                    download(client, SCHEMA, self.output)
                self.assertNotIn(SENTINEL, str(caught.exception))
                self.assertTrue(client.stream.closed)
                self.assert_clean()

    def test_json_exception_row_and_plain_error_fail(self):
        for tail in (b'{"exception":"Code: 241. DB::Exception: Memory limit exceeded"}\n',
                     b"Code: 241. DB::Exception: Memory limit exceeded"):
            with self.subTest(tail=tail[:20]), self.assertRaisesRegex(RuntimeError, "Code: 241"):
                download(FakeClient(row() + tail), SCHEMA, self.output)
            self.assert_clean()

    def test_malformed_rows_fail_without_echoing_content(self):
        good = row()
        cases = {
            "truncated": good + good[:-30],
            "extra column": row(extra=1),
            "missing column": good.replace(b'"trace_id":"",', b""),
            "array": b"[" + good[:-1] + b"]\n",
            "invalid utf8": good.replace(SENTINEL.encode(), SENTINEL.encode() + b"\xff"),
            "blank line": good + b"\n" + good,
            "not json": good.replace(b'"is_deleted":0', b'"is_deleted":0x'),
        }
        for name, body in cases.items():
            with self.subTest(name=name):
                client = FakeClient(body, chunk=11)
                with self.assertRaisesRegex(ValueError, "nothing was written") as caught:
                    download(client, SCHEMA, self.output)
                self.assertNotIn(SENTINEL, str(caught.exception))
                self.assertTrue(client.stream.closed)
                self.assert_clean()

    def test_stream_exception_cleans_up(self):
        client = FakeClient(row() * 3)

        def broken(size):
            raise ConnectionError("connection reset mid-stream")
        original = client.raw_stream

        def raw_stream(*args, **kwargs):
            stream = original(*args, **kwargs)
            stream.read = broken
            return stream
        client.raw_stream = raw_stream
        with self.assertRaises(ConnectionError):
            download(client, SCHEMA, self.output)
        self.assertTrue(client.stream.closed)
        self.assert_clean()

    def test_existing_output_requires_overwrite_before_querying(self):
        self.output.write_text("previous\n")
        client = FakeClient(row())
        with self.assertRaises(FileExistsError):
            download(client, SCHEMA, self.output)
        self.assertEqual(client.calls, [])
        self.assertEqual(self.output.read_text(), "previous\n")
        with self.assertRaises(ValueError):
            download(FakeClient(row(0) + b"oops"), SCHEMA, self.output, overwrite=True)
        self.assertEqual(self.output.read_text(), "previous\n")
        self.assertEqual(download(FakeClient(row()), SCHEMA, self.output, overwrite=True), 1)
        self.assertEqual(self.output.read_bytes(), row())

    def test_invalid_arguments_fail_before_output_or_query(self):
        client = FakeClient(row())
        for kwargs in ({"limit": 0}, {"timeout": 0}, {"batch_size": 0}, {"since": datetime(2026, 1, 1)}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                download(client, SCHEMA, self.output, **kwargs)
        self.assertEqual(client.calls, [])
        self.assert_clean()


if __name__ == "__main__":
    unittest.main()
