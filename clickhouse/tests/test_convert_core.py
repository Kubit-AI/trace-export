"""Core Langfuse events -> OTLP conversion tests on synthetic rows; no server or real traces."""
import base64
import copy
from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from trace_export.convert import main
from trace_export.langfuse import convert_row
from trace_export.reader import decode_attributes
from trace_export.validate import validate_record, validator

FIXTURE = json.loads((Path(__file__).with_name("fixtures") / "events_full_schema.json").read_text())
SENTINEL = "SENTINEL-ROW-CONTENT"
TRACE, SPAN, PARENT = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331", "00f067aa0ba902b7"


def column_default(column_type):
    if column_type.startswith("Nullable("):
        return None
    if column_type.startswith("Array("):
        return []
    if column_type.startswith("Map("):
        return {}
    if column_type.startswith("Decimal("):
        return "0"
    if column_type in ("Bool",):
        return False
    if column_type.startswith("UInt") or column_type.startswith("Int"):
        return 0
    return ""


def make_row(**values):
    # Server-shaped JSONEachRow: every exported column, UTC ISO times, quoted decimals.
    row = {name: column_default(column_type) for name, column_type in FIXTURE["columns"] if name != "ingestion_api_key"}
    row.update(
        project_id="project-a", trace_id=TRACE, span_id=SPAN, parent_span_id=PARENT, name="retrieve documents",
        type="SPAN", environment="default", level="DEFAULT",
        start_time="2026-01-02T03:04:05.123456Z", end_time="2026-01-02T03:04:06.000001Z",
        event_ts="2026-01-02T03:04:07.000000Z", created_at="2026-01-02T03:04:07.000000Z",
        updated_at="2026-01-02T03:04:07.000000Z", input=f"{SENTINEL} input", output=f"{SENTINEL} output",
        metadata_names=["attributes.gen_ai.request.model", "attributes.retry.count", "resourceAttributes.service.name",
                        "resourceAttributes.host.cpu.count", "scope.name", "scope.attributes.lib", "customer.tier"],
        metadata_values=["model-x", "3", "demo-service", "8", "demo-scope", "x", "gold"],
        scope_name="demo-scope", scope_version="1.2.3", service_name="demo-service",
        usage_details={"input": 18446744073709551615, "output": 7},
        cost_details={"total": "0.000000000001"}, calculated_total_cost="123456.789012345678",
        input_length=18446744073709551615, output_length=9223372036854775807, event_bytes=12)
    row.update(values)
    return row


def span_of(record):
    return record["resourceSpans"][0]["scopeSpans"][0]["spans"][0]


def span_attrs(record):
    return decode_attributes(span_of(record)["attributes"])


def resource_attrs(record):
    return decode_attributes(record["resourceSpans"][0]["resource"]["attributes"])


def scope_of(record):
    return record["resourceSpans"][0]["scopeSpans"][0]["scope"]


class ConvertCoreTests(unittest.TestCase):
    def test_identity_timing_kind_and_status(self):
        record, issues = convert_row(make_row())
        span = span_of(record)
        self.assertEqual(issues, [])
        self.assertEqual(len(record["resourceSpans"]), 1)
        self.assertEqual(len(record["resourceSpans"][0]["scopeSpans"][0]["spans"]), 1)
        self.assertEqual((span["traceId"], span["spanId"], span["parentSpanId"]), (TRACE, SPAN, PARENT))
        self.assertEqual(span["name"], "retrieve documents")
        self.assertEqual((span["startTimeUnixNano"], span["endTimeUnixNano"]),
                         ("1767323045123456000", "1767323046000001000"))
        self.assertEqual(span["kind"], 0)
        self.assertEqual(span["status"], {"code": 0})

    def test_published_contract_and_otlp_protobuf(self):
        from google.protobuf.json_format import ParseDict
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        check = validator()
        for row in (make_row(), make_row(parent_span_id="", level="ERROR", status_message="boom")):
            record, _ = convert_row(row)
            self.assertEqual(validate_record(record, check), 1)
            # Round trip through JSON as the CLI writes it.
            wire = json.loads(json.dumps(record, allow_nan=False))
            span = span_of(wire)
            # OTLP JSON uses hex IDs; generic protobuf JSON parsers use base64.
            for key in ("traceId", "spanId", "parentSpanId"):
                if key in span:
                    span[key] = base64.b64encode(bytes.fromhex(span[key])).decode()
            request = ParseDict(wire, ExportTraceServiceRequest())
            parsed = request.resource_spans[0].scope_spans[0].spans[0]
            self.assertEqual(parsed.trace_id.hex(), TRACE)
            self.assertEqual(parsed.start_time_unix_nano, 1767323045123456000)

    def test_restored_attributes_resource_and_scope(self):
        record, _ = convert_row(make_row())
        attrs = span_attrs(record)
        # Langfuse stored attribute values as strings; they stay verbatim strings.
        self.assertEqual(attrs["gen_ai.request.model"], "model-x")
        self.assertEqual(attrs["retry.count"], "3")
        self.assertEqual(resource_attrs(record), {"service.name": "demo-service", "host.cpu.count": "8",
                                                  "langfuse.project.id": "project-a"})
        # Other metadata (scope copy, user metadata) is not promoted; it stays in the row.
        self.assertEqual(set(attrs), {"gen_ai.request.model", "retry.count", "trace_export.langfuse.row"})
        self.assertEqual(attrs["trace_export.langfuse.row"]["metadata_names"][-1], "customer.tier")
        scope = scope_of(record)
        self.assertEqual((scope["name"], scope["version"]), ("demo-scope", "1.2.3"))
        self.assertEqual(decode_attributes(scope["attributes"]), {
            "trace_export.gen_ai.semconv_version": "1.39.0", "trace_export.mapping.version": "1.0.0"})

    def test_scope_fallback_and_empty_metadata(self):
        record, _ = convert_row(make_row(scope_name="", scope_version="", metadata_names=[], metadata_values=[]))
        self.assertEqual(scope_of(record)["name"], "agentic-data-export.langfuse")
        self.assertNotIn("version", scope_of(record))
        self.assertEqual(set(span_attrs(record)), {"trace_export.langfuse.row"})
        self.assertEqual(resource_attrs(record), {"langfuse.project.id": "project-a"})
        row = make_row()
        for column in ("scope_name", "scope_version"):
            del row[column]
        self.assertEqual(scope_of(convert_row(row)[0])["name"], "agentic-data-export.langfuse")

    def test_ordinary_span_has_no_genai_mapping(self):
        # GenAI mapping is covered in test_genai_mapping; ordinary types only keep restored attributes.
        attrs = span_attrs(convert_row(make_row(level="ERROR", session_id="s", provided_model_name="m"))[0])
        self.assertFalse([key for key in attrs if key.startswith("gen_ai.") and key != "gen_ai.request.model"])
        self.assertNotIn("error.type", attrs)
        attrs = span_attrs(convert_row(make_row(type="GENERATION", provided_model_name="m"))[0])
        self.assertEqual(attrs["gen_ai.operation.name"], "chat")

    def test_source_row_preserved_with_exact_numbers_and_no_ingestion_key(self):
        row = make_row(ingestion_api_key="hashed-credential")
        record, _ = convert_row(row)
        preserved = span_attrs(record)["trace_export.langfuse.row"]
        self.assertNotIn("ingestion_api_key", preserved)
        self.assertNotIn("hashed-credential", json.dumps(record))
        self.assertEqual(set(preserved), {name for name, _ in FIXTURE["columns"]} - {"ingestion_api_key"})
        # Beyond int64: exact decimal digits as strings; in range: OTLP intValue.
        self.assertEqual(preserved["input_length"], "18446744073709551615")
        self.assertEqual(preserved["usage_details"], {"input": "18446744073709551615", "output": 7})
        self.assertEqual(preserved["output_length"], 2**63 - 1)
        # Quoted ClickHouse decimals stay exact strings.
        self.assertEqual((preserved["calculated_total_cost"], preserved["cost_details"]),
                         ("123456.789012345678", {"total": "0.000000000001"}))
        self.assertEqual((preserved["start_time"], preserved["end_time"]),
                         ("2026-01-02T03:04:05.123456Z", "2026-01-02T03:04:06.000001Z"))
        self.assertEqual(preserved["input"], f"{SENTINEL} input")
        self.assertIsNone(preserved["completion_start_time"])
        row_attr = next(a for a in span_of(record)["attributes"] if a["key"] == "trace_export.langfuse.row")
        values = {item["key"]: item["value"] for item in row_attr["value"]["kvlistValue"]["values"]}
        self.assertEqual(values["input_length"], {"stringValue": "18446744073709551615"})
        self.assertEqual(values["event_bytes"], {"intValue": "12"})
        self.assertEqual(values["completion_start_time"], {})
        self.assertIn("ingestion_api_key", row)

    def test_root_parent_omitted_and_self_parent_rejected(self):
        for parent in ("", None, "0000000000000000"):
            with self.subTest(parent=parent):
                self.assertNotIn("parentSpanId", span_of(convert_row(make_row(parent_span_id=parent))[0]))
        with self.assertRaisesRegex(ValueError, "own parent"):
            convert_row(make_row(parent_span_id=SPAN))

    def test_strict_ids(self):
        cases = {
            "trace_id": ("0" * 32, TRACE[:-1], TRACE + "0", TRACE + "-1", "g" * 32, "t-" + TRACE, None, 5),
            "span_id": ("0" * 16, SPAN[:-1], SPAN + "-1", "z" * 16, "", None),
            "parent_span_id": (PARENT + "-1", PARENT[:-1], "0" * 15, "x" * 16, 7),
        }
        for column, values in cases.items():
            for value in values:
                with self.subTest(column=column, value=value), self.assertRaises(ValueError):
                    convert_row(make_row(**{column: value}))

    def test_timestamps(self):
        span = span_of(convert_row(make_row(start_time="1970-01-01T00:00:00.000001Z",
                                            end_time="1970-01-01T00:00:00.000002Z"))[0])
        self.assertEqual((span["startTimeUnixNano"], span["endTimeUnixNano"]), ("1000", "2000"))
        for column in ("start_time", "end_time"):
            for value in (None, "not-a-time", 1767323045):
                with self.subTest(column=column, value=value), self.assertRaisesRegex(ValueError, column):
                    convert_row(make_row(**{column: value}))
        row = make_row()
        del row["end_time"]
        with self.assertRaisesRegex(ValueError, "end_time missing"):
            convert_row(row)

    def test_reversed_time_becomes_flagged_zero_duration(self):
        row = make_row(end_time="2026-01-02T03:04:05.123455Z")
        record, issues = convert_row(row)
        span, attrs = span_of(record), span_attrs(record)
        self.assertEqual(issues, ["span_timing_corrected"])
        self.assertEqual(span["endTimeUnixNano"], span["startTimeUnixNano"])
        self.assertEqual((span["spanId"], span["parentSpanId"]), (SPAN, PARENT))
        self.assertTrue(attrs["trace_export.timing.corrected"])
        self.assertEqual(attrs["trace_export.timing.reason"], "end_precedes_start")
        self.assertEqual(attrs["trace_export.langfuse.row"]["end_time"], row["end_time"])
        validate_record(record, validator())

    def test_level_error_sets_status_others_unset(self):
        self.assertEqual(span_of(convert_row(make_row(level="ERROR", status_message="boom"))[0])["status"],
                         {"code": 2, "message": "boom"})
        self.assertEqual(span_of(convert_row(make_row(level="ERROR"))[0])["status"], {"code": 2})
        for level in ("DEFAULT", "DEBUG", "WARNING", "error", ""):
            with self.subTest(level=level):
                status = span_of(convert_row(make_row(level=level, status_message="note"))[0])["status"]
                self.assertEqual(status, {"code": 0})
        with self.assertRaises(ValueError):
            convert_row(make_row(level=None))

    def test_metadata_reconstruction_rejections(self):
        cases = {
            "differ in length": {"metadata_names": ["attributes.a"], "metadata_values": []},
            "duplicate attributes key": {"metadata_names": ["attributes.a", "attributes.a"], "metadata_values": ["1", "2"]},
            "duplicate resourceAttributes key": {"metadata_names": ["resourceAttributes.a", "resourceAttributes.a"],
                                                 "metadata_values": ["1", "1"]},
            "array of strings": {"metadata_names": ["attributes.a"], "metadata_values": [1]},
            "metadata_names must": {"metadata_names": None},
            "conflicts": {"metadata_names": ["resourceAttributes.langfuse.project.id"], "metadata_values": ["other"]},
        }
        for message, values in cases.items():
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                convert_row(make_row(**values))
        for key in ("trace_export.langfuse.row", "trace_export.timing.corrected"):
            row = make_row(metadata_names=["attributes." + key], metadata_values=["x"],
                           end_time="2026-01-02T03:04:05.000000Z")
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "conflicts"):
                convert_row(row)
        # The same key under both prefixes is not a duplicate.
        record, _ = convert_row(make_row(metadata_names=["attributes.k", "resourceAttributes.k"],
                                         metadata_values=["span", "resource"]))
        self.assertEqual((span_attrs(record)["k"], resource_attrs(record)["k"]), ("span", "resource"))

    def test_required_fields(self):
        for values in ({"name": ""}, {"name": None}, {"project_id": ""}, {"project_id": None}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                convert_row(make_row(**values))
        for row in (None, [], "row"):
            with self.subTest(row=row), self.assertRaises(ValueError):
                convert_row(row)

    def test_input_row_not_mutated(self):
        row = make_row(end_time="2026-01-02T03:04:05.000000Z")
        original = copy.deepcopy(row)
        convert_row(row)
        self.assertEqual(row, original)


class ConverterCliTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.source, self.output = Path(self.folder.name) / "raw.jsonl", Path(self.folder.name) / "otel.jsonl"
        bad = make_row(trace_id="not-a-trace-id")
        lines = [make_row(), bad, make_row(span_id="1111111111111111", ingestion_api_key="hashed-credential")]
        self.source.write_text("".join(json.dumps(line, ensure_ascii=False) + "\n" for line in lines), encoding="utf-8")
        self.args = ["--input", str(self.source), "--output", str(self.output), "--overwrite"]

    def test_strict_failure_then_explicit_skip(self):
        self.output.write_text("original")
        with redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(main([*self.args, "--source", "clickhouse"]), 1)
        self.assertIn("line 2: invalid trace ID", errors.getvalue())
        self.assertNotIn(SENTINEL, errors.getvalue())
        self.assertEqual(self.output.read_text(), "original")
        with redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(main([*self.args, "--source", "clickhouse", "--skip-invalid"]), 0)
        self.assertIn("Skipped line 2", errors.getvalue())
        self.assertIn("Converted 2 spans from 3 rows", errors.getvalue())
        self.assertNotIn(SENTINEL, errors.getvalue())
        lines = self.output.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 2)
        self.assertNotIn("hashed-credential", self.output.read_text(encoding="utf-8"))
        check = validator()
        for line in lines:
            validate_record(json.loads(line), check)
        self.assertEqual(sorted(p.name for p in Path(self.folder.name).iterdir()), ["otel.jsonl", "raw.jsonl"])

    def test_standalone_wrapper_selects_clickhouse(self):
        wrapper = ROOT / "clickhouse" / "convert_to_otel.py"
        result = subprocess.run([sys.executable, str(wrapper), *self.args, "--skip-invalid"],
                                capture_output=True, text=True, cwd=self.folder.name, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Converted 2 spans from 3 rows", result.stderr)
        first = json.loads(self.output.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(span_of(first)["traceId"], TRACE)
        self.assertEqual(scope_of(first)["name"], "demo-scope")
        result = subprocess.run([sys.executable, str(wrapper), *self.args], capture_output=True, text=True,
                                cwd=self.folder.name, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn("line 2", result.stderr)


if __name__ == "__main__":
    unittest.main()
