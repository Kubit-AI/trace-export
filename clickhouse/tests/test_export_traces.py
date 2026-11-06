"""ClickHouse exporter CLI tests use fake clients, dummy credentials and synthetic rows; they never contact a server."""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import base64
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("clickhouse_export", ROOT / "clickhouse" / "export_traces.py")
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)

FIXTURE = json.loads((Path(__file__).with_name("fixtures") / "events_full_schema.json").read_text())
SENTINEL = "SENTINEL-ROW-CONTENT"
PASSWORD = "s3cret-value"
BASIC = "cmVhZGVyOnMzY3JldC12YWx1ZQ=="  # base64("reader:s3cret-value")
TRACE = "0af7651916cd43dd8448eb211c80319c"
TemporaryDirectory = tempfile.TemporaryDirectory


def preflight(timeout):
    return ('{"time":"2001-02-03T04:05:06.789012Z","uint":18446744073709551615,"int":-9223372036854775808,'
            '"decimal":"-1.000000000001","uints":{"k":18446744073709551615},"decimals":{"k":"0.000000000001"},'
            f'"utf8":"a�","max_execution_time":"{timeout}","timeout_overflow_mode":"throw"}}\n').encode()


def default(column_type):
    if column_type.startswith("Nullable("):
        return None
    if column_type.startswith("Array("):
        return []
    if column_type.startswith("Map("):
        return {}
    if column_type.startswith(("UInt", "Int")):
        return 0
    return "0" if column_type.startswith("Decimal(") else ""


def row(index=0, **values):
    data = {name: default(kind) for name, kind in FIXTURE["columns"] if name != "ingestion_api_key"}
    data.update(project_id="project-a", trace_id=TRACE, span_id=f"{index + 1:016x}", name=f"step {index}",
                type="SPAN", level="DEFAULT", start_time="2026-01-02T03:04:05.123456Z",
                end_time="2026-01-02T03:04:06.000001Z", event_ts="2026-01-02T03:04:07.000000Z",
                input=f"{SENTINEL} input", output=f"{SENTINEL} output")
    data.update(values)
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"


class Stream:
    def __init__(self, body, failure=None):
        self.body, self.failure, self.closed = body, failure, False

    def read(self, size):
        if self.failure and not self.body:
            raise self.failure
        data, self.body = self.body[:size], self.body[size:]
        return data

    def close(self):
        self.closed = True


class FakeClient:
    def __init__(self, body=b"", table=FIXTURE["table"], errors=None):
        self.body, self.table, self.errors = body, table, errors or {}
        self.calls, self.closed, self.stream = [], False, None

    def fail(self, kind):
        self.calls.append(kind)
        if kind in self.errors:
            raise self.errors[kind]

    def query(self, sql, parameters=None):
        self.fail("query")
        return types.SimpleNamespace(result_rows=[self.table] if "system.tables" in sql else FIXTURE["columns"])

    def raw_query(self, sql, settings=None, fmt=None):
        self.fail("raw_query")
        return preflight(settings["max_execution_time"])

    def raw_stream(self, sql, parameters=None, settings=None, fmt=None):
        self.fail("raw_stream")
        self.sql, self.parameters, self.settings = sql, parameters, settings
        self.stream = Stream(self.body, self.errors.get("read"))
        return self.stream

    def close(self):
        self.closed = True


class ExportTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.env_file = self.folder / "settings.env"
        # Langfuse headless-init keys must never become an implicit project filter.
        self.env_file.write_text(f"CLICKHOUSE_URL=http://127.0.0.1:8123\nCLICKHOUSE_USER=reader\n"
                                 f"CLICKHOUSE_PASSWORD={PASSWORD}\nLANGFUSE_INIT_PROJECT_ID=init-project\n")
        environment = patch.dict(os.environ, {"LANGFUSE_INIT_PROJECT_ID": "init-project"}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def run_export(self, client, *options, connect=None):
        with ExitStack() as stack:
            if connect is None:
                connect = stack.enter_context(patch.object(exporter, "connect", return_value=client))
            stack.enter_context(patch.object(exporter.tempfile, "TemporaryDirectory", self.temporary_directory))
            stdout = stack.enter_context(redirect_stdout(io.StringIO()))
            stderr = stack.enter_context(redirect_stderr(io.StringIO()))
            result = exporter.main(["--env-file", str(self.env_file), *options])
        return result, stdout.getvalue(), stderr.getvalue()

    def temporary_directory(self, prefix):
        # Track the exporter's scratch directory to prove it is removed.
        self.scratch = TemporaryDirectory(prefix=prefix, dir=self.folder)
        return self.scratch

    def assert_scratch_removed(self):
        self.assertFalse(Path(self.scratch.name).exists())

    def test_default_sample_is_global_bounded_and_streams_otlp_to_stdout(self):
        client = FakeClient(row(0) + row(1))
        before = datetime.now(timezone.utc)
        result, out, err = self.run_export(client)
        self.assertEqual(result, 0)
        records = [json.loads(line) for line in out.splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["traceId"], TRACE)
        self.assertEqual(client.calls, ["query", "query", "raw_query", "raw_stream"])
        self.assertNotIn("project_id", client.parameters)
        self.assertNotIn("init-project", client.sql)
        self.assertTrue(client.sql.endswith(" LIMIT 10"))
        self.assertNotIn("ORDER BY", client.sql)
        self.assertEqual((client.settings["max_execution_time"], client.settings["max_block_size"]), (300, 1000))
        since = datetime.fromisoformat(client.parameters["since"] + "+00:00")
        until = datetime.fromisoformat(client.parameters["until"] + "+00:00")
        self.assertEqual(until - since, timedelta(hours=24))
        self.assertLessEqual(before - until, timedelta(seconds=1))
        self.assertIn("Project scope: all projects readable by this ClickHouse user; the row limit is global.", err)
        self.assertIn("query timeout=300s", err)
        self.assertTrue(client.closed and client.stream.closed)
        self.assert_scratch_removed()

    def test_project_filter_and_explicit_options(self):
        client = FakeClient(row())
        output = self.folder / "out.jsonl"
        result, out, err = self.run_export(
            client, "--project-id", "project-a", "--no-sample", "--since", "2026-01-01T02:00:00+02:00",
            "--until", "2026-01-03T00:00:00Z", "--order-by-time", "--batch-size", "7", "--query-timeout", "42",
            "--output", str(output), "--table", "events_full", "--database", "langfuse")
        self.assertEqual(result, 0)
        self.assertEqual(out, "")
        self.assertEqual(client.parameters, {"project_id": "project-a", "since": "2026-01-01 00:00:00.000000",
                                             "until": "2026-01-03 00:00:00.000000"})
        self.assertIn("FROM `langfuse`.`events_full` FINAL", client.sql)
        self.assertIn(" ORDER BY `start_time`", client.sql)
        self.assertNotIn("LIMIT", client.sql)
        self.assertEqual((client.settings["max_execution_time"], client.settings["max_block_size"]), (42, 7))
        self.assertIn("Project scope: project project-a.", err)
        self.assertEqual(len(output.read_text().splitlines()), 1)

    def test_settings_come_from_env_file_and_cli_overrides(self):
        client = FakeClient()
        with patch.object(exporter, "connect", return_value=client) as connect:
            result, _, _ = self.run_export(client, "--url", "https://ch.example.com:8443", "--database", "other_db",
                                           connect=connect)
        self.assertEqual(result, 0)
        values = connect.call_args.args[0]
        self.assertEqual((values["CLICKHOUSE_URL"], values["CLICKHOUSE_DB"], values["CLICKHOUSE_USER"]),
                         ("https://ch.example.com:8443", "other_db", "reader"))
        self.assertNotIn("LANGFUSE_INIT_PROJECT_ID", values)
        self.assertEqual(connect.call_args.kwargs, {"timeout": 300})

    def test_raw_format_keeps_every_observation_type_verbatim(self):
        body = b"".join(row(index, type=kind) for index, kind in
                        enumerate(("SPAN", "GENERATION", "AGENT", "TOOL", "EVENT", "CUSTOM_TYPE")))
        client = FakeClient(body)
        result, out, err = self.run_export(client, "--format", "raw")
        self.assertEqual((result, out.encode()), (0, body))
        self.assertIn("Raw export written to stdout", err)
        output = self.folder / "raw.jsonl"
        result, _, _ = self.run_export(FakeClient(body), "--format", "raw", "--output", str(output))
        self.assertEqual((result, output.read_bytes()), (0, body))

    def test_keep_raw_retains_download_and_temporary_raw_is_removed(self):
        output, raw = self.folder / "out.jsonl", self.folder / "kept.raw.jsonl"
        result, _, err = self.run_export(FakeClient(row()), "--output", str(output), "--keep-raw", str(raw))
        self.assertEqual(result, 0)
        self.assertEqual(raw.read_bytes(), row())
        self.assertIn("Raw download retained", err)
        self.assert_scratch_removed()

    def test_strict_conversion_failures_preserve_output_and_print_nothing(self):
        output = self.folder / "out.jsonl"
        output.write_text("previous\n")
        cases = {"missing end": row(end_time=None), "uuid trace": row(trace_id="0af76519-16cd-43dd-8448-eb211c80319c"),
                 "suffixed span": row(span_id="b7ad6b7169203331-1"), "short trace": row(trace_id="abc")}
        for name, body in cases.items():
            for options in ((), ("--output", str(output), "--overwrite", "--json-summary")):
                with self.subTest(name=name, options=options):
                    result, out, err = self.run_export(FakeClient(row(0) + body), *options)
                    self.assertEqual(result, 1)
                    self.assertIn("Conversion failed: line 2:", err)
                    self.assertNotIn(SENTINEL, err)
                    if options:
                        summary = json.loads(out)
                        self.assertEqual((summary["source"], summary["status"], summary["output_written"],
                                          summary["downloaded_rows"]), ("clickhouse", "error", False, 2))
                    else:
                        self.assertEqual(out, "")
                    self.assertEqual(output.read_text(), "previous\n")
                    self.assert_scratch_removed()
        self.assertEqual(sorted(path.name for path in self.folder.iterdir()), ["out.jsonl", "settings.env"])

    def test_json_summary_reports_clickhouse_source(self):
        output = self.folder / "out.jsonl"
        result, out, _ = self.run_export(FakeClient(row()), "--output", str(output), "--json-summary", "--limit", "1")
        summary = json.loads(out)
        self.assertEqual(result, 0)
        self.assertEqual((summary["source"], summary["status"], summary["downloaded_rows"], summary["format"]),
                         ("clickhouse", "ok", 1, "otel"))
        self.assertTrue(summary["output_written"] and summary["possibly_truncated"] and summary["sample"])
        self.assertEqual(summary["query_timeout_seconds"], 300)

    def test_driver_errors_are_redacted_and_clients_closed(self):
        secret = RuntimeError(f"HTTPDriver for https://reader:{PASSWORD}@ch.example.com:8443 failed; "
                              f"Authorization: Basic {BASIC}; password={PASSWORD}; proxy http://u:p@proxy")
        for stage in ("query", "raw_query", "raw_stream", "read"):
            with self.subTest(stage=stage):
                output = self.folder / "out.jsonl"
                client = FakeClient(row(), errors={stage: secret})
                result, out, err = self.run_export(client, "--output", str(output), "--json-summary")
                self.assertEqual(result, 1)
                self.assertEqual(json.loads(out)["status"], "error")
                self.assertIn("Export failed: HTTPDriver for https://<redacted>@ch.example.com:8443", err)
                for leaked in (PASSWORD, BASIC, "u:p@"):
                    self.assertNotIn(leaked, err)
                self.assertTrue(client.closed)
                self.assertFalse(output.exists())
                self.assert_scratch_removed()

    def test_password_containing_at_is_redacted_from_errors(self):
        # Synthetic credential: URL redaction alone would leave "dummy-suffix@..." behind.
        password = "dummy-prefix@dummy-suffix"
        basic = base64.b64encode(f"reader:{password}".encode()).decode()
        self.env_file.write_text(f"CLICKHOUSE_URL=http://127.0.0.1:8123\nCLICKHOUSE_USER=reader\n"
                                 f"CLICKHOUSE_PASSWORD={password}\n")
        secret = RuntimeError(f"HTTPDriver for https://reader:{password}@ch.example.com:8443 failed; "
                              f"Authorization: Basic {basic}; password={password}")
        driver = types.SimpleNamespace(get_client=MagicMock(side_effect=secret))
        for stage in ("connect", "raw_stream"):
            with self.subTest(stage=stage), patch.dict(sys.modules, {"clickhouse_connect": driver}):
                if stage == "connect":
                    result, out, err = self.run_export(None, connect=exporter.connect)
                else:
                    result, out, err = self.run_export(FakeClient(row(), errors={stage: secret}))
                self.assertEqual((result, out), (1, ""))
                self.assertIn("https://<redacted>@ch.example.com:8443", err)
                for leaked in (password, basic, "dummy-prefix", "dummy-suffix"):
                    self.assertNotIn(leaked, err)

    def test_connection_failure_is_redacted(self):
        driver = types.SimpleNamespace(get_client=MagicMock(side_effect=RuntimeError(
            f"connect https://reader:{PASSWORD}@ch.example.com auth {BASIC}")))
        with patch.dict(sys.modules, {"clickhouse_connect": driver}):
            result, out, err = self.run_export(None, connect=exporter.connect)
        self.assertEqual((result, out), (1, ""))
        self.assertIn("Export failed: ClickHouse connection failed", err)
        self.assertNotIn(PASSWORD, err)
        self.assertNotIn(BASIC, err)

    def test_mid_stream_server_error_does_not_echo_rows(self):
        body = row(0) + row(1)[:50] + b"\nCode: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)\n"
        result, out, err = self.run_export(FakeClient(body))
        self.assertEqual((result, out), (1, ""))
        self.assertIn("TIMEOUT_EXCEEDED", err)
        self.assertNotIn(SENTINEL, err)

    def test_schema_and_settings_are_checked_before_data_query(self):
        unsupported = ["MergeTree", "MergeTree ORDER BY x", "project_id", ""]
        client = FakeClient(row(), table=unsupported)
        result, out, err = self.run_export(client)
        self.assertEqual((result, out), (1, ""))
        self.assertIn("expected Langfuse v4 ReplacingMergeTree", err)
        self.assertEqual(client.calls, ["query"])
        self.assertTrue(client.closed)
        client = FakeClient(row())
        client.raw_query = lambda *args, **kwargs: preflight(300).replace(b'"throw"', b'"break"')
        result, out, err = self.run_export(client)
        self.assertEqual((result, out), (1, ""))
        self.assertIn("did not apply the required export settings (timeout_overflow_mode)", err)
        self.assertNotIn("raw_stream", client.calls)

    def test_invalid_options_fail_before_connecting(self):
        existing = self.folder / "existing.jsonl"
        existing.write_text("previous\n")
        for options in (["--project-id", ""], ["--table", "events_full FINAL"], ["--database", "a;b"],
                        ["--format", "raw", "--keep-raw", str(self.folder / "k")],
                        ["--output", str(existing)], ["--output", str(self.folder / "o"), "--keep-raw", str(existing)],
                        ["--output", str(existing), "--keep-raw", str(existing), "--overwrite"],
                        ["--no-sample", "--output", str(self.folder / "o")], ["--limit", "0"], ["--batch-size", "0"],
                        ["--since", "2026-01-01T00:00:00"]):
            with self.subTest(options=options), patch.object(exporter, "connect") as connect, \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                exporter.main(["--env-file", str(self.env_file), *options])
            self.assertEqual(caught.exception.code, 2)
            connect.assert_not_called()
        self.assertEqual(existing.read_text(), "previous\n")

    def test_missing_settings_and_dependencies(self):
        self.env_file.write_text("CLICKHOUSE_USER=reader\n")
        result, out, err = self.run_export(FakeClient())
        self.assertEqual((result, out), (1, ""))
        self.assertIn("Missing settings: CLICKHOUSE_URL", err)
        self.env_file.write_text(f"CLICKHOUSE_URL=http://reader:{PASSWORD}@host:8123\nCLICKHOUSE_USER=reader\n")
        result, _, err = self.run_export(FakeClient())
        self.assertEqual(result, 1)
        self.assertNotIn(PASSWORD, err)
        with patch.object(exporter, "connect", side_effect=ImportError("No module named 'clickhouse_connect'")), \
                redirect_stderr(io.StringIO()) as stderr, self.assertRaises(SystemExit) as caught:
            exporter.main(["--env-file", str(self.env_file), "--url", "http://host:8123"])
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("pip install -r", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
