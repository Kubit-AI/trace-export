"""Profile authentication tests use dummy credentials and never contact Databricks."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from trace_export.databricks_source import connect, settings


class ConnectionTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.profile = Path(self.folder.name) / "profiles.ini"
        self.profile.write_text(
            "[traces]\nhost = https://example.cloud.databricks.com\n"
            "token = dummy-profile-token\nwarehouse_id = warehouse123\n"
            "[other]\nhost = https://other.cloud.databricks.com\ntoken = other-token\n")
        self.env = patch.dict(os.environ, {"DATABRICKS_CONFIG_FILE": str(self.profile)}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        # Recent SDK versions probe workspace metadata during Config construction.
        network = patch("requests.Session.request", side_effect=RuntimeError("offline test"))
        network.start()
        self.addCleanup(network.stop)

    def test_named_profile_ignores_dotenv_and_supplies_refreshable_auth(self):
        dotenv = Path(self.folder.name) / ".env"
        dotenv.write_text("DATABRICKS_SERVER_HOSTNAME=wrong\nDATABRICKS_TOKEN=wrong\n")
        with patch("dotenv.dotenv_values", side_effect=AssertionError("must not read .env")):
            values = settings(dotenv, connection="traces")
        with patch("databricks.sql.connect") as sql_connect:
            connect(values)
        args = sql_connect.call_args.kwargs
        self.assertEqual(args["server_hostname"], "example.cloud.databricks.com")
        self.assertEqual(args["http_path"], "/sql/1.0/warehouses/warehouse123")
        self.assertNotIn("access_token", args)
        authenticate = args["credentials_provider"]()
        self.assertEqual(authenticate()["Authorization"], "Bearer dummy-profile-token")
        with patch.object(values["sdk_config"], "authenticate", return_value={"Authorization": "refreshed"}):
            self.assertEqual(args["credentials_provider"]()(), {"Authorization": "refreshed"})

    def test_http_path_override_and_missing_warehouse(self):
        with self.assertRaisesRegex(ValueError, "warehouse_id.*--http-path"):
            settings(connection="other")
        values = settings(connection="other", http_path="/sql/1.0/warehouses/override")
        self.assertEqual(values["DATABRICKS_HTTP_PATH"], "/sql/1.0/warehouses/override")
        self.assertEqual(values["DATABRICKS_SERVER_HOSTNAME"], "other.cloud.databricks.com")

    def test_unknown_profile_fails(self):
        with self.assertRaisesRegex(ValueError, "no missing profile"):
            settings(connection="missing")

    def test_sdk_environment_precedence(self):
        os.environ["DATABRICKS_TOKEN"] = "environment-token"
        os.environ["DATABRICKS_WAREHOUSE_ID"] = "environment-warehouse"
        values = settings(connection="traces")
        self.assertEqual(values["sdk_config"].authenticate()["Authorization"], "Bearer environment-token")
        self.assertEqual(values["DATABRICKS_HTTP_PATH"], "/sql/1.0/warehouses/environment-warehouse")

    def test_cli_rejects_connection_with_explicit_env_file(self):
        spec = importlib.util.spec_from_file_location("databricks_export_profiles", ROOT / "databricks/export_traces.py")
        exporter = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(exporter)
        with patch("sys.stderr"), self.assertRaises(SystemExit) as caught:
            exporter.parser().parse_args(["--connection", "traces", "--env-file", "dev.env", "--list-agents"])
        self.assertEqual(caught.exception.code, 2)
