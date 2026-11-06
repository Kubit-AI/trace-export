"""ClickHouse settings and schema tests use dummy credentials and never contact a server."""
import base64
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from trace_export.clickhouse_source import connect, inspect_schema, redact, settings

FIXTURE = json.loads((Path(__file__).with_name("fixtures") / "events_full_schema.json").read_text())
# Synthetic credential containing @: URL redaction must not leave the part after it.
AT_PASSWORD = "dummy-prefix@dummy-suffix"
AT_BASIC = base64.b64encode(f"reader:{AT_PASSWORD}".encode()).decode()


class FakeClient:
    def __init__(self, table=FIXTURE["table"], columns=FIXTURE["columns"]):
        self.table, self.columns, self.calls = table, columns, []

    def query(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        rows = ([self.table] if self.table else []) if "system.tables" in sql else self.columns
        return types.SimpleNamespace(result_rows=rows)


class SettingsTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.env_file = Path(self.folder.name) / ".env"
        self.env_file.write_text(
            "CLICKHOUSE_URL=http://file-host:8123\nCLICKHOUSE_USER=file-user\n"
            "CLICKHOUSE_PASSWORD=pa$$${HOME}$(touch should-not-run)\nCLICKHOUSE_DB=file_db\n"
            "LANGFUSE_INIT_PROJECT_ID=local-project\n")
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_file_values_are_literal_and_unrelated_keys_dropped(self):
        values = settings(self.env_file)
        self.assertEqual(values["CLICKHOUSE_PASSWORD"], "pa$$${HOME}$(touch should-not-run)")
        self.assertFalse((Path.cwd() / "should-not-run").exists())
        self.assertEqual(values["CLICKHOUSE_DB"], "file_db")
        # No implicit project filter from Langfuse headless-init settings.
        self.assertEqual(set(values), {"CLICKHOUSE_URL", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DB"})

    def test_precedence_file_then_environment_then_cli(self):
        os.environ.update(CLICKHOUSE_URL="https://env-host", CLICKHOUSE_USER="env-user", CLICKHOUSE_DB="env_db")
        values = settings(self.env_file)
        self.assertEqual((values["CLICKHOUSE_URL"], values["CLICKHOUSE_USER"], values["CLICKHOUSE_DB"]),
                         ("https://env-host", "env-user", "env_db"))
        values = settings(self.env_file, url="http://127.0.0.1:8123", database="cli_db")
        self.assertEqual((values["CLICKHOUSE_URL"], values["CLICKHOUSE_DB"]), ("http://127.0.0.1:8123", "cli_db"))
        self.assertEqual(values["CLICKHOUSE_USER"], "env-user")

    def test_defaults_and_missing_settings(self):
        with patch("trace_export.clickhouse_source.DEFAULT_ENV", Path(self.folder.name) / "absent.env"):
            with self.assertRaisesRegex(ValueError, "Missing settings: CLICKHOUSE_URL, CLICKHOUSE_USER"):
                settings()
            os.environ.update(CLICKHOUSE_URL="http://localhost:8123", CLICKHOUSE_USER="reader")
            values = settings()
        self.assertEqual((values["CLICKHOUSE_DB"], values["CLICKHOUSE_PASSWORD"]), ("default", ""))
        with self.assertRaisesRegex(ValueError, "Env file not found"):
            settings(Path(self.folder.name) / "explicit-missing.env")

    def test_invalid_urls_and_database_do_not_echo_secrets(self):
        for url, error in (("http://user:hunter2@host:8123", "credentials"), ("ftp://host", "http:// or https://"),
                           ("http://host:8123/proxy", "path"), ("http://host:99999", "valid URL"),
                           ("host:8123", "http:// or https://")):
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, error) as caught:
                settings(self.env_file, url=url)
            self.assertNotIn("hunter2", str(caught.exception))
        with self.assertRaisesRegex(ValueError, "identifiers"):
            settings(self.env_file, database="default; DROP TABLE x")

    def test_ca_certificate_requires_https_and_existing_file(self):
        ca = Path(self.folder.name) / "ca.crt"
        ca.write_text("dummy")
        os.environ["CLICKHOUSE_CA_CERT"] = str(ca)
        with self.assertRaisesRegex(ValueError, "requires an https"):
            settings(self.env_file)
        self.assertEqual(settings(self.env_file, url="https://host:8443")["CLICKHOUSE_CA_CERT"], str(ca))
        os.environ["CLICKHOUSE_CA_CERT"] = str(ca) + ".missing"
        with self.assertRaisesRegex(ValueError, "file not found"):
            settings(self.env_file, url="https://host:8443")


class ConnectTests(unittest.TestCase):
    values = {"CLICKHOUSE_URL": "https://ch.example.com", "CLICKHOUSE_USER": "reader",
              "CLICKHOUSE_PASSWORD": "s3cret-value", "CLICKHOUSE_DB": "default"}

    def driver(self, **kwargs):
        module = types.SimpleNamespace(get_client=MagicMock(**kwargs))
        return patch.dict(sys.modules, {"clickhouse_connect": module}), module.get_client

    def test_https_keeps_verification_and_passes_ca(self):
        installed, get_client = self.driver()
        with installed:
            connect({**self.values, "CLICKHOUSE_CA_CERT": "/tmp/ca.crt"})
            connect({**self.values, "CLICKHOUSE_URL": "http://127.0.0.1:8123"}, timeout=30)
        secure, plain = (call.kwargs for call in get_client.call_args_list)
        self.assertEqual((secure["host"], secure["port"], secure["secure"], secure["verify"], secure["ca_cert"]),
                         ("ch.example.com", 443, True, True, "/tmp/ca.crt"))
        self.assertEqual((secure["username"], secure["database"]), ("reader", "default"))
        self.assertEqual((plain["host"], plain["port"], plain["secure"], plain["ca_cert"], plain["send_receive_timeout"]),
                         ("127.0.0.1", 8123, False, None, 30))

    def test_connection_errors_are_sanitized_without_chained_exception(self):
        failure = RuntimeError("bad https://reader:s3cret-value@ch.example.com/ auth cmVhZGVyOnMzY3JldC12YWx1ZQ== "
                               "s3cret-value proxy http://u:p@proxy")
        installed, _ = self.driver(side_effect=failure)
        with installed, self.assertRaises(RuntimeError) as caught:
            connect(self.values)
        message = str(caught.exception)
        for secret in ("s3cret-value", "cmVhZGVyOnMzY3JldC12YWx1ZQ==", "u:p@"):
            self.assertNotIn(secret, message)
        self.assertIn("https://<redacted>@ch.example.com", message)
        self.assertIsNone(caught.exception.__cause__)
        self.assertTrue(caught.exception.__suppress_context__)

    def test_redact_without_password_still_strips_url_credentials(self):
        self.assertEqual(redact("http://a:b@h:8123 ok", {}), "http://<redacted>@h:8123 ok")
        # Userinfo runs to the authority's last @; later @ in a path or query is not userinfo.
        self.assertEqual(redact(f"https://reader:{AT_PASSWORD}@h:8443/x?to=a@b mail a@b"),
                         "https://<redacted>@h:8443/x?to=a@b mail a@b")
        self.assertEqual(redact("http://h:8123/p@q http://h?u=a@b"), "http://h:8123/p@q http://h?u=a@b")

    def test_password_containing_at_is_fully_redacted(self):
        values = {**self.values, "CLICKHOUSE_PASSWORD": AT_PASSWORD}
        message = redact(f"https://reader:{AT_PASSWORD}@ch.example.com/ Basic {AT_BASIC} pw={AT_PASSWORD}.", values)
        self.assertEqual(message, "https://<redacted>@ch.example.com/ Basic <redacted> pw=<redacted>.")
        failure = RuntimeError(f"bad https://reader:{AT_PASSWORD}@ch.example.com auth {AT_BASIC} {AT_PASSWORD}")
        installed, _ = self.driver(side_effect=failure)
        with installed, self.assertRaises(RuntimeError) as caught:
            connect(values)
        message = str(caught.exception)
        for secret in (AT_PASSWORD, AT_BASIC, "dummy-prefix", "dummy-suffix"):
            self.assertNotIn(secret, message)
        self.assertIn("https://<redacted>@ch.example.com", message)


class SchemaTests(unittest.TestCase):
    def test_langfuse_v4_fixture_is_supported(self):
        client = FakeClient()
        schema = inspect_schema(client)
        self.assertEqual((schema["database"], schema["table"], schema["engine"]),
                         ("default", "events_full", "ReplacingMergeTree"))
        self.assertEqual((schema["version_column"], schema["is_deleted_column"]), ("event_ts", "is_deleted"))
        self.assertEqual(schema["missing_optional_columns"], [])
        self.assertEqual(schema["columns"]["start_time"], "DateTime64(6)")
        self.assertEqual(client.calls[0][1], {"database": "default", "table": "events_full"})
        self.assertTrue(all("{database:String}" in sql for sql, _ in client.calls))

    def test_replicated_engine_arguments(self):
        engine_full = ("ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/events_full', '{replica}', "
                       "event_ts, is_deleted) ORDER BY (project_id, trace_id, span_id, start_time)")
        table = ["ReplicatedReplacingMergeTree", engine_full, "project_id, trace_id, span_id, start_time", ""]
        self.assertEqual(inspect_schema(FakeClient(table=table))["engine"], "ReplicatedReplacingMergeTree")

    def test_missing_table(self):
        with self.assertRaisesRegex(ValueError, "default.events_full was not found"):
            inspect_schema(FakeClient(table=None))

    def test_engine_and_replacement_semantics(self):
        engine, engine_full, sorting_key, partition_key = FIXTURE["table"]
        cases = (
            (["MergeTree", "MergeTree ORDER BY x", sorting_key, partition_key], "uses MergeTree"),
            (["ReplacingMergeTree", "ReplacingMergeTree(event_ts) ORDER BY x", sorting_key, partition_key],
             r"ReplacingMergeTree\(event_ts, is_deleted\)"),
            (["ReplacingMergeTree", "ReplacingMergeTree ORDER BY x", sorting_key, partition_key], "replacement"),
            ([engine, engine_full, "project_id, xxHash32(trace_id), start_time", partition_key], "lacks span_id"),
        )
        for table, error in cases:
            with self.subTest(error=error), self.assertRaisesRegex(ValueError, error):
                inspect_schema(FakeClient(table=table))

    def test_sorting_key_must_be_stable_observation_identity(self):
        engine, engine_full, sorting_key, partition_key = FIXTURE["table"]
        self.assertEqual(sorting_key, "project_id, toStartOfMinute(start_time), xxHash32(trace_id), span_id, start_time")
        for accepted in (sorting_key, "project_id, trace_id, span_id, start_time",
                         "project_id, xxHash32(trace_id), trace_id, span_id, start_time"):
            with self.subTest(accepted=accepted):
                inspect_schema(FakeClient(table=[engine, engine_full, accepted, partition_key]))
        # Mutable columns would keep superseded or deleted versions apart under FINAL;
        # substituted expressions would merge distinct spans or split one span.
        unsupported = {
            sorting_key + ", event_ts": "event_ts",
            sorting_key + ", is_deleted": "is_deleted",
            "event_ts, " + sorting_key: "event_ts",
            sorting_key + ", toStartOfMinute(event_ts)": r"toStartOfMinute\(event_ts\)",
            sorting_key + ", name": "name",
            sorting_key.replace("span_id", "cityHash64(span_id)"): r"cityHash64\(span_id\)",
            sorting_key.replace("xxHash32(trace_id)", "xxHash64(trace_id)"): r"xxHash64\(trace_id\)",
            sorting_key.replace(", start_time", ", toStartOfHour(start_time)"): r"toStartOfHour\(start_time\)",
            "project_id, trace_id, span_id, start_time) , event_ts": r"start_time\)",
            "project_id, trace_id, tuple(span_id, start_time)": r"tuple\(span_id",
            "project_id, `trace id`, span_id, start_time": "`trace id`",
        }
        for key, expression in unsupported.items():
            with self.subTest(key=key), self.assertRaisesRegex(
                    ValueError, f"sorting key has unsupported expressions .*{expression}"):
                inspect_schema(FakeClient(table=[engine, engine_full, key, partition_key]))
        missing = {
            "project_id, toStartOfMinute(start_time), xxHash32(trace_id), span_id": "start_time",
            "toStartOfMinute(start_time), xxHash32(trace_id), span_id, start_time": "project_id",
            "": "project_id, trace_id, span_id, start_time",
        }
        for key, columns in missing.items():
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, f"sorting key lacks {columns};"):
                inspect_schema(FakeClient(table=[engine, engine_full, key, partition_key]))

    def test_column_validation(self):
        columns = dict(FIXTURE["columns"])
        del columns["span_id"], columns["tool_calls"]
        columns["is_deleted"] = "Bool"
        columns["usage_details"] = "Map(String, Float64)"
        with self.assertRaises(ValueError) as caught:
            inspect_schema(FakeClient(columns=list(columns.items())))
        message = str(caught.exception)
        for problem in ("span_id missing", "is_deleted has type Bool", "usage_details has type Map(String, Float64)"):
            self.assertIn(problem, message)
        self.assertNotIn("tool_calls", message)

    def test_optional_columns_reported_and_compatible_variants_accepted(self):
        columns = dict(FIXTURE["columns"])
        del columns["tool_calls"]
        columns["start_time"] = "DateTime64(3, 'UTC')"
        columns["trace_id"] = "LowCardinality(String)"
        schema = inspect_schema(FakeClient(columns=list(columns.items())))
        self.assertEqual(schema["missing_optional_columns"], ["tool_calls"])

    def test_identifiers_are_validated_before_querying(self):
        client = FakeClient()
        with self.assertRaisesRegex(ValueError, "identifiers"):
            inspect_schema(client, table="events_full FINAL")
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
