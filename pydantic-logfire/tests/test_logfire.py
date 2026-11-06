"""Logfire exporter tests use synthetic rows and a fake query client; they never contact Logfire."""
from contextlib import ExitStack, redirect_stderr, redirect_stdout
import copy
import importlib.util
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(ROOT))
from trace_export.logfire import convert_row
from trace_export.reader import decode_attributes
from trace_export.validate import validate_record, validator

spec = importlib.util.spec_from_file_location("logfire_export", ROOT / "pydantic-logfire" / "export_traces.py")
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)

ROWS = json.loads((Path(__file__).with_name("fixtures") / "records.json").read_text())
TOKEN = "pylf_v2_eu_dummy-token-value"


def span_of(record):
    return record["resourceSpans"][0]["scopeSpans"][0]["spans"][0]


class ConvertTests(unittest.TestCase):
    def setUp(self):
        self.check = validator()

    def test_rows_convert_to_valid_records(self):
        converted = [convert_row(row) for row in ROWS]
        self.assertIsNone(converted[-1][0])  # pending span
        for record, _ in converted[:-1]:
            self.assertEqual(validate_record(record, self.check), 1)

    def test_span_fields_and_restored_logfire_attributes(self):
        record, issues = convert_row(ROWS[0])
        self.assertEqual(issues, [])
        span = span_of(record)
        self.assertNotIn("parentSpanId", span)
        self.assertEqual(span["startTimeUnixNano"], "1767323045000001000")
        self.assertEqual(span["kind"], 0)
        attrs = decode_attributes(span["attributes"])
        self.assertEqual(attrs["logfire.span_type"], "span")
        self.assertEqual(attrs["logfire.tags"], ["demo"])
        self.assertEqual(attrs["logfire.level_num"], 9)
        self.assertIsInstance(attrs["logfire.json_schema"], str)
        self.assertEqual(attrs["gen_ai.aggregated_usage.input_tokens"], 120)
        resource = decode_attributes(record["resourceSpans"][0]["resource"]["attributes"])
        self.assertEqual(resource["logfire.project.id"], "example-project")
        self.assertEqual(resource["service.name"], "example-agent")
        scope = record["resourceSpans"][0]["scopeSpans"][0]["scope"]
        self.assertEqual(scope["name"], "pydantic-ai")
        self.assertEqual(decode_attributes(scope["attributes"])["trace_export.mapping.version"], "1.0.0")

    def test_tool_call_response_result_is_renamed(self):
        record, issues = convert_row(ROWS[3])
        self.assertEqual(issues, [])
        messages = decode_attributes(span_of(record)["attributes"])["gen_ai.input.messages"]
        part = messages[2]["parts"][0]
        self.assertEqual(part["response"], [{"detail": "Included."}])
        self.assertNotIn("result", part)
        self.assertIn("result", ROWS[3]["attributes"]["gen_ai.input.messages"][2]["parts"][0])  # input untouched

    def test_log_error_events_and_links(self):
        log = span_of(convert_row(ROWS[4])[0])
        self.assertEqual(log["startTimeUnixNano"], log["endTimeUnixNano"])
        self.assertEqual(decode_attributes(log["attributes"])["logfire.msg"], "Answered breakfast")
        record, issues = convert_row(ROWS[5])
        self.assertEqual(issues, ["gen_ai_messages_not_schema_compatible:gen_ai.input.messages"])
        span = span_of(record)
        self.assertEqual(span["status"], {"code": 2, "message": "Content filter triggered"})
        self.assertEqual(decode_attributes(span["attributes"])["error.type"], "ContentFilterError")
        self.assertEqual(span["events"][0]["name"], "exception")
        self.assertEqual(span["links"][0]["spanId"], "7777777777777777")

    def test_unknown_link_shape_is_kept(self):
        row = copy.deepcopy(ROWS[5])
        row["otel_links"] = [{"something": "else"}]
        record, issues = convert_row(row)
        self.assertIn("links_unmapped", issues)
        self.assertNotIn("links", span_of(record))
        self.assertEqual(decode_attributes(span_of(record)["attributes"])["trace_export.logfire.otel_links"],
                         [{"something": "else"}])

    def test_rejects_bad_rows(self):
        for key, value in [("trace_id", "xyz"), ("span_id", "0" * 15), ("start_timestamp", "yesterday"),
                           ("otel_status_code", "MAYBE"), ("parent_span_id", ROWS[1]["span_id"])]:
            row = copy.deepcopy(ROWS[1])
            row[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                convert_row(row)

    def test_reversed_timing_is_corrected(self):
        row = copy.deepcopy(ROWS[1])
        row["end_timestamp"] = "2026-01-02T03:04:05Z"
        record, issues = convert_row(row)
        span = span_of(record)
        self.assertEqual(span["endTimeUnixNano"], span["startTimeUnixNano"])
        self.assertIn("span_timing_corrected", issues)

    def test_example_matches_fixture_conversion(self):
        expected = [record for record, _ in map(convert_row, ROWS) if record]
        published = [json.loads(line) for line in (ROOT / "examples/logfire.otel.jsonl").read_text().splitlines()]
        self.assertEqual(published, expected)


class FakeClient:
    """Evaluates the exporter's keyset SQL over in-memory rows."""

    def __init__(self, rows, failures=()):
        self.rows = sorted((r for r in rows if r["kind"] != "pending_span"), key=lambda r: exporter.row_key(r)[:3])
        self.failures = list(failures)
        self.calls = []
        self.base_url = "https://logfire.example"

    def info(self):
        return {"organization_name": "org", "project_name": "proj"}

    def query_json_rows(self, sql, min_timestamp, max_timestamp=None, limit=None, environment=None):
        self.calls.append({"sql": sql, "limit": limit, "min": min_timestamp, "max": max_timestamp})
        if self.failures:
            raise self.failures.pop(0)
        match = re.search(r"start_timestamp > TIMESTAMP '([^']+)'.*trace_id > '(\w+)'.*span_id > '(\w+)'", sql)
        rows = self.rows
        if match:
            cursor = exporter.row_key({"start_timestamp": match[1], "trace_id": match[2], "span_id": match[3]})[:3]
            rows = [r for r in rows if exporter.row_key(r)[:3] > cursor]
        return {"columns": [], "rows": copy.deepcopy(rows[:limit])}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def tied_rows(count):
    """Rows sharing one start timestamp, to exercise tie-breaking."""
    rows = []
    for index in range(count):
        row = copy.deepcopy(ROWS[1])
        row["span_id"] = f"{index + 1:016x}"
        rows.append(row)
    return rows


class ExportTests(unittest.TestCase):
    def run_cli(self, args, client, env_token=TOKEN):
        with ExitStack() as stack:
            stack.enter_context(patch.dict("os.environ", {exporter.TOKEN_KEY: env_token}, clear=False))
            stack.enter_context(patch("logfire.query_client.LogfireQueryClient", return_value=client))
            stack.enter_context(patch.object(exporter.time, "sleep"))
            err = stack.enter_context(redirect_stderr(io.StringIO()))
            out = stack.enter_context(redirect_stdout(io.StringIO()))
            result = exporter.main(args)
        return result, out.getvalue(), err.getvalue()

    def export_args(self, folder, *extra):
        return ["--no-sample", "--since", "2026-01-01T00:00:00Z", "--until", "2026-01-03T00:00:00Z",
                "--output", str(Path(folder) / "out.jsonl"), *extra]

    def test_paginates_ties_without_loss_or_duplicates(self):
        rows = tied_rows(25)
        client = FakeClient(rows)
        with tempfile.TemporaryDirectory() as folder:
            result, _, _ = self.run_cli(self.export_args(folder, "--page-size", "4", "--format", "raw"), client)
            self.assertEqual(result, 0)
            ids = [json.loads(line)["span_id"] for line in (Path(folder) / "out.jsonl").read_text().splitlines()]
        self.assertEqual(ids, sorted(r["span_id"] for r in rows))
        self.assertEqual(len(client.calls), 8)  # 7 pages of up to 4, then an empty page
        self.assertIn("kind != 'pending_span'", client.calls[0]["sql"])
        self.assertIn("start_timestamp < TIMESTAMP '2026-01-03T00:00:00.000000Z'", client.calls[0]["sql"])

    def test_otel_output_validates(self):
        client = FakeClient(ROWS)
        check = validator()
        with tempfile.TemporaryDirectory() as folder:
            result, out, _ = self.run_cli(self.export_args(folder, "--json-summary"), client)
            self.assertEqual(result, 0)
            lines = (Path(folder) / "out.jsonl").read_text().splitlines()
        self.assertEqual(len(lines), 6)
        for line in lines:
            validate_record(json.loads(line), check)
        summary = json.loads(out)
        self.assertEqual((summary["source"], summary["downloaded_rows"]), ("logfire", 6))

    def test_retries_shrink_page_and_honor_rate_limits(self):
        import httpx
        from logfire.query_client import QueryRateLimitedError
        limited = QueryRateLimitedError("Unexpected response status code 429: ''")
        limited.retry_after = 7.0
        client = FakeClient(tied_rows(5), failures=[httpx.ReadTimeout("slow"), limited])
        with tempfile.TemporaryDirectory() as folder, patch.object(exporter.time, "sleep") as sleep:
            with patch.dict("os.environ", {exporter.TOKEN_KEY: TOKEN}), \
                    patch("logfire.query_client.LogfireQueryClient", return_value=client), \
                    redirect_stderr(io.StringIO()):
                result = exporter.main(self.export_args(folder, "--page-size", "4", "--format", "raw"))
            self.assertEqual(result, 0)
            self.assertEqual(len((Path(folder) / "out.jsonl").read_text().splitlines()), 5)
        self.assertEqual([c["limit"] for c in client.calls[:3]], [4, 2, 2])
        self.assertEqual(sleep.call_args_list[1].args, (7.0,))

    def test_non_retryable_error_fails_without_leaking_token(self):
        from logfire.query_client import UnexpectedResponseError
        client = FakeClient(ROWS, failures=[UnexpectedResponseError(f"status code 401: bad token {TOKEN}")])
        with tempfile.TemporaryDirectory() as folder:
            result, _, err = self.run_cli(self.export_args(folder), client)
        self.assertEqual(result, 1)
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn(TOKEN, err)
        self.assertIn("[REDACTED]", err)

    def test_gives_up_after_max_retries(self):
        import httpx
        client = FakeClient(ROWS, failures=[httpx.ConnectError("down")] * 3)
        with tempfile.TemporaryDirectory() as folder:
            result, _, _ = self.run_cli(self.export_args(folder, "--max-retries", "2"), client)
        self.assertEqual(result, 1)
        self.assertEqual(len(client.calls), 3)

    def test_resume_continues_after_partial_line(self):
        rows = tied_rows(10)
        with tempfile.TemporaryDirectory() as folder:
            raw = Path(folder) / "raw.jsonl"
            args = self.export_args(folder, "--keep-raw", str(raw), "--page-size", "3")
            # A run that fails after saving some rows leaves the raw file and state behind.
            import httpx
            client = FakeClient(rows, failures=[])
            original = client.query_json_rows
            def flaky(*a, **k):
                if len(client.calls) == 2:
                    client.calls.append({})
                    raise httpx.ConnectError("down")
                return original(*a, **k)
            client.query_json_rows = flaky
            result, _, err = self.run_cli([*args, "--max-retries", "0"], client)
            self.assertEqual(result, 1)
            self.assertIn("--resume", err)
            self.assertTrue(exporter.state_path(raw).is_file())
            with raw.open("a") as handle:
                handle.write('{"partial":')
            client = FakeClient(rows)
            result, _, err = self.run_cli([*args, "--resume"], client)
            self.assertEqual(result, 0, err)
            ids = [json.loads(line)["span_id"] for line in raw.read_text().splitlines()]
            self.assertEqual(ids, sorted(r["span_id"] for r in rows))
            self.assertEqual(len((Path(folder) / "out.jsonl").read_text().splitlines()), 10)
            self.assertFalse(exporter.state_path(raw).exists())

    def test_resume_rejects_changed_window(self):
        with tempfile.TemporaryDirectory() as folder:
            raw = Path(folder) / "raw.jsonl"
            raw.write_text("")
            exporter.state_path(raw).write_text(json.dumps({"version": 1, "since": "x", "until": "y", "environment": None}))
            result, _, err = self.run_cli(self.export_args(folder, "--keep-raw", str(raw), "--resume"), FakeClient(ROWS))
        self.assertEqual(result, 1)
        self.assertIn("must match", err)

    def test_limit_and_sample_default(self):
        client = FakeClient(tied_rows(30))
        result, out, _ = self.run_cli(["--format", "raw"], client)
        self.assertEqual(result, 0)
        self.assertEqual(len(out.splitlines()), 10)
        self.assertEqual(client.calls[0]["limit"], 10)

    def test_missing_token(self):
        with tempfile.TemporaryDirectory() as folder:
            result, _, err = self.run_cli(self.export_args(folder), FakeClient(ROWS), env_token="")
        self.assertEqual(result, 1)
        self.assertIn("Missing LOGFIRE_READ_TOKEN", err)

    def test_sql_rejects_non_hex_cursor(self):
        with self.assertRaises(ValueError):
            exporter.row_key({"start_timestamp": "2026-01-01T00:00:00Z", "trace_id": "x' OR 1=1 --", "span_id": "ab"})

    def test_from_raw_converts_locally(self):
        with tempfile.TemporaryDirectory() as folder:
            raw = Path(folder) / "raw.jsonl"
            raw.write_text("".join(json.dumps(row) + "\n" for row in ROWS))
            with redirect_stderr(io.StringIO()):
                result = exporter.main(["--from-raw", str(raw), "--output", str(Path(folder) / "o.jsonl")])
            self.assertEqual(result, 0)
            self.assertEqual(len((Path(folder) / "o.jsonl").read_text().splitlines()), 6)


if __name__ == "__main__":
    unittest.main()
