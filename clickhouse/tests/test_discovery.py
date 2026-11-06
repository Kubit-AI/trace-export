"""ClickHouse discovery tests use fake clients and dummy credentials; they never contact a server."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from trace_export.discovery import clickhouse_discover, main

FIXTURE = json.loads((Path(__file__).with_name("fixtures") / "events_full_schema.json").read_text())
PASSWORD = "s3cret-value"


class FakeClient:
    """Answers system.tables/system.columns metadata queries; any other query fails the test."""

    def __init__(self, databases, tables=None, columns=None, failures=None):
        self.databases, self.tables, self.columns = databases, tables or {}, columns or {}
        self.failures, self.calls, self.closed = failures or {}, [], False

    def query(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        assert " FROM system.tables " in sql or " FROM system.columns " in sql, sql
        if "SELECT database, name" in sql:
            rows = [(db, "events_full") for db in self.databases
                    if parameters.get("database") in (None, db)]
            return types.SimpleNamespace(result_rows=rows)
        database = parameters["database"]
        if database in self.failures:
            raise self.failures[database]
        if "system.tables" in sql:
            return types.SimpleNamespace(result_rows=[self.tables.get(database, FIXTURE["table"])])
        return types.SimpleNamespace(result_rows=self.columns.get(database, FIXTURE["columns"]))

    def close(self):
        self.closed = True


class DiscoverTests(unittest.TestCase):
    def test_validates_each_visible_events_full_table(self):
        merge_tree = ["MergeTree", "MergeTree ORDER BY x", "project_id, trace_id, span_id, start_time", ""]
        columns = [column for column in FIXTURE["columns"] if column[0] != "tool_calls"]
        client = FakeClient(
            ["langfuse", "plain", "blocked", "partial_cols", "bad-name"],
            tables={"plain": merge_tree}, columns={"partial_cols": columns},
            failures={"blocked": RuntimeError(f"auth https://reader:{PASSWORD}@host failed")})
        result = clickhouse_discover(client)
        items = {item["database"]: item for item in result["items"]}
        self.assertEqual([item["qualified_name"] for item in result["items"]], [
            "bad-name.events_full", "blocked.events_full", "langfuse.events_full",
            "partial_cols.events_full", "plain.events_full"])
        self.assertEqual(items["langfuse"]["validation"], "schema_supported")
        self.assertEqual(items["langfuse"]["export_args"], ["--database", "langfuse", "--table", "events_full"])
        self.assertEqual(items["langfuse"]["missing_optional_columns"], [])
        self.assertEqual(items["partial_cols"]["missing_optional_columns"], ["tool_calls"])
        self.assertEqual(items["plain"]["validation"], "unsupported_schema")
        self.assertIn("uses MergeTree", items["plain"]["schema_problem"])
        self.assertIsNone(items["plain"]["export_args"])
        self.assertEqual(items["blocked"]["validation"], "unverified")
        self.assertEqual(items["blocked"]["export_args"], ["--database", "blocked", "--table", "events_full"])
        self.assertEqual((items["bad-name"]["validation"], items["bad-name"]["export_args"]), ("candidate", None))
        self.assertEqual(sorted(result["issues"], key=lambda entry: entry["scope"]), [
            {"scope": "bad-name.events_full", "code": "identifier_not_supported_by_exporter"},
            {"scope": "blocked.events_full", "code": "schema_check_failed", "error_type": "RuntimeError"}])
        self.assertEqual((result["status"], result["complete"], result["source"]), ("partial", False, "clickhouse"))
        self.assertNotIn(PASSWORD, json.dumps(result))

    def test_schema_problem_redacts_password_containing_at(self):
        # Discovery has no settings to match, so the URL authority is redacted through its last @.
        failure = ValueError("schema check via https://reader:dummy-prefix@dummy-suffix@host:8443/ failed")
        result = clickhouse_discover(FakeClient(["langfuse"], failures={"langfuse": failure}))
        item = result["items"][0]
        self.assertEqual(item["validation"], "unsupported_schema")
        self.assertEqual(item["schema_problem"], "schema check via https://<redacted>@host:8443/ failed")
        self.assertNotIn("dummy-", json.dumps(result))

    def test_metadata_only_queries_with_bound_parameters(self):
        client = FakeClient(["langfuse"])
        clickhouse_discover(client)
        listing, *inspection = client.calls
        self.assertEqual(listing[1], {"table": "events_full"})
        self.assertNotIn("database =", listing[0])
        self.assertTrue(all(parameters == {"database": "langfuse", "table": "events_full"}
                            for _, parameters in inspection))
        statements = " ".join(sql for sql, _ in client.calls)
        for forbidden in ("events_core", "FINAL", "project_id =", "langfuse.events_full", "`events_full`"):
            self.assertNotIn(forbidden, statements)

    def test_database_filter_and_database_name_match(self):
        client = FakeClient(["langfuse_prod", "langfuse_dev", "other"])
        result = clickhouse_discover(client, database="langfuse_dev")
        self.assertEqual([item["database"] for item in result["items"]], ["langfuse_dev"])
        self.assertEqual(client.calls[0][1], {"table": "events_full", "database": "langfuse_dev"})
        self.assertIn("AND database = {database:String}", client.calls[0][0])
        result = clickhouse_discover(FakeClient(["langfuse_prod", "langfuse_dev", "other"]), match="LANGFUSE_*")
        self.assertEqual([item["database"] for item in result["items"]], ["langfuse_dev", "langfuse_prod"])
        self.assertTrue(result["complete"])

    def test_no_candidates_is_a_complete_search(self):
        result = clickhouse_discover(FakeClient([]))
        self.assertEqual((result["status"], result["complete"], result["items"]), ("ok", True, []))


class MainTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.env_file = Path(folder.name) / "settings.env"
        self.env_file.write_text(f"CLICKHOUSE_URL=http://127.0.0.1:8123\nCLICKHOUSE_USER=reader\n"
                                 f"CLICKHOUSE_PASSWORD={PASSWORD}\nCLICKHOUSE_DB=session_db\n")
        environment = patch.dict(os.environ, {}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def run_main(self, options, client=None, **connect):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch("trace_export.clickhouse_source.connect", return_value=client, **connect) as patched, \
                redirect_stdout(stdout), redirect_stderr(stderr):
            code = main("clickhouse", ["--env-file", str(self.env_file), *options])
        return code, stdout.getvalue(), stderr.getvalue(), patched

    def test_json_result_uses_env_settings_and_closes_client(self):
        client = FakeClient(["langfuse"])
        code, out, _, connect = self.run_main(["--url", "http://localhost:8124", "--database", "langfuse",
                                               "--json"], client)
        self.assertEqual(code, 0)
        result = json.loads(out)
        self.assertEqual(result["items"][0]["export_args"], ["--database", "langfuse", "--table", "events_full"])
        self.assertIn("hidden by permissions", result["visibility_note"])
        values = connect.call_args.args[0]
        # --database narrows the search only; the session keeps the configured database.
        self.assertEqual((values["CLICKHOUSE_URL"], values["CLICKHOUSE_DB"]), ("http://localhost:8124", "session_db"))
        self.assertTrue(client.closed)

    def test_human_output_is_default(self):
        merge_tree = ["MergeTree", "MergeTree ORDER BY x", "project_id, trace_id, span_id, start_time", ""]
        code, out, _, _ = self.run_main([], FakeClient(["langfuse", "plain"], tables={"plain": merge_tree}))
        self.assertEqual(code, 0)
        self.assertIn("langfuse.events_full [Langfuse v4 schema supported; read access not checked]", out)
        self.assertIn("plain.events_full [not a supported Langfuse v4 events table]\n  plain.events_full uses MergeTree",
                      out)
        self.assertIn("Found 2 matching objects; search complete.", out)

    def test_empty_search_differs_from_failed_search(self):
        code, out, _, _ = self.run_main(["--json"], FakeClient([]))
        empty = json.loads(out)
        self.assertEqual((code, empty["status"], empty["complete"]), (0, "ok", True))
        self.assertIn("hidden by permissions", empty["empty_result_note"])
        self.assertIn("SELECT on <database>.events_full", " ".join(empty["permission_guidance"]))
        failure = RuntimeError(f"ClickHouse connection failed: https://reader:{PASSWORD}@host")
        code, out, err, _ = self.run_main(["--json"], side_effect=failure)
        failed = json.loads(out)
        self.assertEqual((code, failed["status"], failed["complete"]), (1, "error", False))
        self.assertEqual(failed["issues"], [{"scope": "discovery", "code": "discovery_failed",
                                             "error_type": "RuntimeError"}])
        self.assertIn("discovery failed", failed["empty_result_note"])
        self.assertNotIn(PASSWORD, out + err)

    def test_listing_failure_closes_client_without_leaking(self):
        client = FakeClient([])
        client.query = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError(PASSWORD))
        code, out, err, _ = self.run_main([], client)
        self.assertEqual(code, 1)
        self.assertTrue(client.closed)
        self.assertIn("search failed", out)
        self.assertIn("Discovery failed", err)
        self.assertNotIn(PASSWORD, out + err)

    def test_partial_exit_code(self):
        client = FakeClient(["blocked"], failures={"blocked": PermissionError()})
        code, _, err, _ = self.run_main([], client)
        self.assertEqual(code, 3)
        self.assertIn("blocked.events_full: Could not inspect", err)

    def test_invalid_settings_and_missing_dependency(self):
        self.env_file.write_text(f"CLICKHOUSE_URL=http://reader:{PASSWORD}@host:8123\nCLICKHOUSE_USER=reader\n")
        code, out, err, connect = self.run_main(["--json"], FakeClient([]))
        result = json.loads(out)
        self.assertEqual((code, result["status"], result["issues"][0]["code"]), (1, "error", "settings_invalid"))
        self.assertFalse(connect.called)
        self.assertNotIn(PASSWORD, out + err)
        self.env_file.write_text("CLICKHOUSE_URL=http://host:8123\nCLICKHOUSE_USER=reader\n")
        code, out, err, _ = self.run_main([], side_effect=ImportError("No module named 'clickhouse_connect'"))
        self.assertEqual(code, 1)
        self.assertIn("pip install -r clickhouse/requirements.txt", err)

    def test_vendor_only_options_rejected(self):
        for options in (["--connection", "x"], ["--schema", "s"], ["--catalog", "c"], ["--no-verify"]):
            with self.subTest(options=options), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
                main("clickhouse", options)
            self.assertEqual(exc.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
