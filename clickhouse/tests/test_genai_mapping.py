"""Langfuse observation types -> GenAI v1.39.0 attributes on synthetic rows; no server or real traces."""
import base64
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_convert_core import ROOT, SENTINEL, make_row, span_attrs, span_of
from trace_export.langfuse import convert_row, schema_messages
from trace_export.validate import validate_record, validator

SEMCONV = ROOT / "snowflake" / "tests" / "fixtures" / "semconv-v1.39.0"
INPUT = [{"role": "system", "content": "Be brief."},
         {"role": "user", "content": [{"type": "text", "text": "Weather in Sofia?"}]}]
OUTPUT = {"role": "assistant", "content": None,
          "tool_calls": [{"type": "function", "function": {"name": "weather", "arguments": "{\"city\": \"Sofia\"}"}}]}
STRUCTURED_INPUT = [{"role": "user", "parts": [{"type": "text", "content": "hi"}]}]
STRUCTURED_OUTPUT = [{"role": "assistant", "parts": [{"type": "text", "content": "hello"}], "finish_reason": "stop"}]


def row(**values):
    # No restored metadata unless a test adds it, so mapped values are visible.
    defaults = dict(metadata_names=[], metadata_values=[], usage_details={}, input="", output="")
    return make_row(**{**defaults, **values})


def restored(**attrs):
    return {"metadata_names": ["attributes." + key for key in attrs], "metadata_values": list(attrs.values())}


def generation(**values):
    defaults = dict(type="GENERATION", name="llm call", provided_model_name="model-x", session_id="session-1",
                    input=json.dumps(INPUT), output=json.dumps(OUTPUT),
                    usage_details={"input": 18, "output": 7, "total": 25, "input_cached_tokens": 5},
                    provided_usage_details={"input": 20})
    return row(**{**defaults, **values})


def message_validators():
    from jsonschema import Draft202012Validator
    return {output: Draft202012Validator(json.loads((SEMCONV / f"gen-ai-{direction}-messages.json").read_text()))
            for output, direction in ((False, "input"), (True, "output"))}


class GenAIMappingTests(unittest.TestCase):
    def test_generation_maps_to_chat(self):
        record, issues = convert_row(generation())
        span, attrs = span_of(record), span_attrs(record)
        self.assertEqual(span["name"], "chat model-x")
        self.assertEqual(span["kind"], 0)
        self.assertEqual((attrs["gen_ai.operation.name"], attrs["gen_ai.request.model"], attrs["gen_ai.conversation.id"]),
                         ("chat", "model-x", "session-1"))
        # Effective usage only: no total, cache or provided-usage substitution.
        self.assertEqual((attrs["gen_ai.usage.input_tokens"], attrs["gen_ai.usage.output_tokens"]), (18, 7))
        self.assertNotIn("gen_ai.provider.name", attrs)
        self.assertEqual(attrs["gen_ai.input.messages"], [
            {"role": "system", "parts": [{"type": "text", "content": "Be brief."}]},
            {"role": "user", "parts": [{"type": "text", "content": "Weather in Sofia?"}]}])
        # Missing tool call ID stays null; missing finish reason is explicitly unknown.
        self.assertEqual(attrs["gen_ai.output.messages"], [{"role": "assistant", "finish_reason": "unknown", "parts": [
            {"type": "tool_call", "id": None, "name": "weather", "arguments": {"city": "Sofia"}}]}])
        self.assertEqual(issues, ["output_finish_reason_unavailable"])
        preserved = attrs["trace_export.langfuse.row"]
        self.assertEqual((preserved["provided_usage_details"], preserved["usage_details"]["total"]), ({"input": 20}, 25))
        self.assertEqual(preserved["input"], json.dumps(INPUT))

    def test_generation_without_model_or_usage_keeps_name(self):
        record, issues = convert_row(row(type="GENERATION", output="plain answer"))
        attrs = span_attrs(record)
        self.assertEqual(span_of(record)["name"], "retrieve documents")
        for key in ("gen_ai.request.model", "gen_ai.usage.input_tokens", "gen_ai.conversation.id", "gen_ai.input.messages"):
            self.assertNotIn(key, attrs)
        self.assertEqual(attrs["gen_ai.output.messages"], [
            {"role": "assistant", "parts": [{"type": "text", "content": "plain answer"}], "finish_reason": "unknown"}])
        self.assertEqual(issues, ["output_finish_reason_unavailable"])
        # Finish reasons present in the payload are kept.
        output = json.dumps({"role": "assistant", "content": "done", "finish_reason": "stop"})
        record, issues = convert_row(row(type="GENERATION", output=output))
        self.assertEqual(span_attrs(record)["gen_ai.output.messages"][0]["finish_reason"], "stop")
        self.assertEqual(issues, [])

    def test_invalid_usage_counts_warn(self):
        record, issues = convert_row(generation(usage_details={"input": 2**64 - 1, "output": 7}))
        attrs = span_attrs(record)
        self.assertNotIn("gen_ai.usage.input_tokens", attrs)
        self.assertEqual(attrs["gen_ai.usage.output_tokens"], 7)
        self.assertIn("invalid_token_count:input", issues)
        self.assertEqual(attrs["trace_export.langfuse.row"]["usage_details"]["input"], str(2**64 - 1))
        _, issues = convert_row(generation(usage_details=["input"]))
        self.assertIn("token_usage_unmapped", issues)

    def test_agent_maps_to_invoke_agent(self):
        record, _ = convert_row(row(type="AGENT", name="planner", session_id="session-1", level="ERROR",
                                    input="What now?", output=json.dumps({"role": "assistant", "content": "Plan."})))
        span, attrs = span_of(record), span_attrs(record)
        self.assertEqual(span["name"], "invoke_agent planner")
        self.assertEqual((attrs["gen_ai.operation.name"], attrs["gen_ai.agent.name"], attrs["gen_ai.conversation.id"]),
                         ("invoke_agent", "planner", "session-1"))
        self.assertEqual(attrs["gen_ai.input.messages"], [{"role": "user", "parts": [{"type": "text", "content": "What now?"}]}])
        self.assertEqual(attrs["gen_ai.output.messages"][0]["parts"], [{"type": "text", "content": "Plan."}])
        self.assertEqual((attrs["error.type"], span["status"]["code"]), ("_OTHER", 2))
        self.assertNotIn("gen_ai.usage.input_tokens", span_attrs(convert_row(row(type="AGENT", usage_details={"input": 3}))[0]))

    def test_tool_maps_to_execute_tool(self):
        record, issues = convert_row(row(type="TOOL", name="weather", input=json.dumps({"city": "Sofia"}),
                                         output="Sunny", session_id="session-1"))
        span, attrs = span_of(record), span_attrs(record)
        self.assertEqual(span["name"], "execute_tool weather")
        self.assertEqual((attrs["gen_ai.tool.name"], attrs["gen_ai.tool.call.arguments"], attrs["gen_ai.tool.call.result"]),
                         ("weather", {"city": "Sofia"}, "Sunny"))
        self.assertEqual(attrs["gen_ai.conversation.id"], "session-1")
        for key in ("gen_ai.tool.call.id", "gen_ai.input.messages", "gen_ai.output.messages"):
            self.assertNotIn(key, attrs)
        self.assertEqual(issues, [])
        attrs = span_attrs(convert_row(row(type="TOOL", name="weather", **restored(**{"gen_ai.tool.call.id": "call-1"})))[0])
        self.assertEqual(attrs["gen_ai.tool.call.id"], "call-1")
        self.assertNotIn("gen_ai.tool.call.arguments", attrs)

    def test_other_types_stay_ordinary(self):
        for kind in ("SPAN", "EVENT", "CHAIN", "RETRIEVER", "EMBEDDING", "EVALUATOR", "GUARDRAIL", ""):
            with self.subTest(kind=kind):
                record, issues = convert_row(generation(type=kind, level="ERROR"))
                attrs = span_attrs(record)
                self.assertEqual(span_of(record)["name"], "llm call")
                self.assertEqual(set(attrs), {"trace_export.langfuse.row"})
                self.assertEqual(attrs["trace_export.langfuse.row"]["type"], kind)
                self.assertEqual(issues, [])

    def test_restored_attributes_take_precedence(self):
        values = {"gen_ai.operation.name": "text_completion", "gen_ai.request.model": "restored-model",
                  "gen_ai.provider.name": "openai", "gen_ai.conversation.id": "restored-session",
                  "gen_ai.usage.input_tokens": "42", "gen_ai.input.messages": json.dumps(STRUCTURED_INPUT),
                  "gen_ai.output.messages": json.dumps(STRUCTURED_OUTPUT), "error.type": "timeout"}
        record, issues = convert_row(generation(level="ERROR", **restored(**values)))
        attrs = span_attrs(record)
        self.assertEqual(span_of(record)["name"], "text_completion restored-model")
        self.assertEqual((attrs["gen_ai.operation.name"], attrs["gen_ai.request.model"], attrs["gen_ai.provider.name"]),
                         ("text_completion", "restored-model", "openai"))
        self.assertEqual((attrs["gen_ai.conversation.id"], attrs["error.type"]), ("restored-session", "timeout"))
        # Restored usage wins over usage_details; missing restored counts still map.
        self.assertEqual((attrs["gen_ai.usage.input_tokens"], attrs["gen_ai.usage.output_tokens"]), (42, 7))
        self.assertEqual((attrs["gen_ai.input.messages"], attrs["gen_ai.output.messages"]), (STRUCTURED_INPUT, STRUCTURED_OUTPUT))
        self.assertEqual(issues, [])
        preserved = attrs["trace_export.langfuse.row"]
        self.assertIn(json.dumps(STRUCTURED_INPUT), preserved["metadata_values"])
        self.assertIn("42", preserved["metadata_values"])
        agent = span_attrs(convert_row(row(type="AGENT", name="a", **restored(**{"gen_ai.agent.name": "restored-agent"})))[0])
        self.assertEqual(agent["gen_ai.agent.name"], "restored-agent")

    def test_incompatible_restored_attributes_stay_strings_with_warnings(self):
        missing_reason = [{"role": "assistant", "parts": [{"type": "text", "content": "x"}]}]
        values = {"gen_ai.input.messages": "not json", "gen_ai.output.messages": json.dumps(missing_reason),
                  "gen_ai.usage.input_tokens": str(2**63), "gen_ai.usage.output_tokens": "-1"}
        record, issues = convert_row(generation(**restored(**values)))
        attrs = span_attrs(record)
        for key, value in values.items():
            self.assertEqual(attrs[key], value)
            self.assertIn("existing_attribute_not_schema_compatible:" + key, issues)
        # Restored strings on ordinary spans are normalized too, without adding mappings.
        attrs = span_attrs(convert_row(row(**restored(**{"gen_ai.input.messages": json.dumps(STRUCTURED_INPUT),
                                                         "gen_ai.usage.output_tokens": "5"})))[0])
        self.assertEqual((attrs["gen_ai.input.messages"], attrs["gen_ai.usage.output_tokens"]), (STRUCTURED_INPUT, 5))
        self.assertNotIn("gen_ai.operation.name", attrs)

    def test_out_of_int64_payload_integers_become_exact_strings(self):
        big, low = 2**64 - 1, -(2**63) - 1
        arguments = json.dumps({"id": big, "nested": [{"n": big, "low": low}], "small": 7})
        exact = {"id": str(big), "nested": [{"n": str(big), "low": str(low)}], "small": 7}
        check, validators = validator(), message_validators()

        def converted(source):
            record, issues = convert_row(source)
            self.assertEqual(validate_record(record, check), 1)
            attrs = span_attrs(record)
            for output, key in ((False, "gen_ai.input.messages"), (True, "gen_ai.output.messages")):
                if isinstance(attrs.get(key), list):
                    validators[output].validate(attrs[key])
            return attrs, issues

        # Mapped TOOL arguments and result, including a bare top-level UInt64.
        attrs, issues = converted(row(type="TOOL", name="lookup", input=arguments, output=str(big)))
        self.assertEqual((attrs["gen_ai.tool.call.arguments"], attrs["gen_ai.tool.call.result"]), (exact, str(big)))
        self.assertEqual(issues, [])
        attrs, _ = converted(row(type="TOOL", name="lookup", input=str(big), output=json.dumps([big, {"r": [big]}])))
        self.assertEqual((attrs["gen_ai.tool.call.arguments"], attrs["gen_ai.tool.call.result"]),
                         (str(big), [str(big), {"r": [str(big)]}]))
        self.assertEqual(attrs["trace_export.langfuse.row"]["input"], str(big))
        # Restored tool arguments keep precedence and stay verbatim strings.
        attrs, _ = converted(row(type="TOOL", name="lookup", input=arguments,
                                 **restored(**{"gen_ai.tool.call.arguments": arguments})))
        self.assertEqual(attrs["gen_ai.tool.call.arguments"], arguments)

        # Mapped message tool call arguments and tool responses.
        output = {"role": "assistant", "content": None, "finish_reason": "tool_calls", "tool_calls": [
            {"id": "call-1", "type": "function", "function": {"name": "lookup", "arguments": arguments}}]}
        history = [{"role": "user", "content": [{"type": "tool_use", "id": "call-0", "name": "lookup",
                                                 "input": {"ids": [big]}},
                                                {"type": "tool_result", "tool_use_id": "call-0", "content": {"v": big}}]},
                   {"role": "tool", "tool_call_id": "call-0", "content": [{"type": "text", "text": "ok"}]}]
        attrs, issues = converted(generation(input=json.dumps(history), output=json.dumps(output)))
        self.assertEqual(attrs["gen_ai.output.messages"], [{"role": "assistant", "finish_reason": "tool_calls", "parts": [
            {"type": "tool_call", "id": "call-1", "name": "lookup", "arguments": exact}]}])
        self.assertEqual(attrs["gen_ai.input.messages"][0]["parts"], [
            {"type": "tool_call", "name": "lookup", "id": "call-0", "arguments": {"ids": [str(big)]}},
            {"type": "tool_call_response", "id": "call-0", "response": {"v": str(big)}}])
        self.assertEqual(issues, [])
        self.assertEqual(attrs["trace_export.langfuse.row"]["output"], json.dumps(output))

        # Restored schema-valid messages with nested out-of-range tool arguments and responses.
        restored_output = [{"role": "assistant", "finish_reason": "tool_call", "parts": [
            {"type": "tool_call", "id": "call-2", "name": "lookup", "arguments": json.loads(arguments)},
            {"type": "custom_part", "payload": [big]}]}]
        restored_input = [{"role": "tool", "parts": [{"type": "tool_call_response", "id": "call-2",
                                                      "response": {"rows": [[big, low]]}}]}]
        values = {"gen_ai.output.messages": json.dumps(restored_output), "gen_ai.input.messages": json.dumps(restored_input)}
        attrs, issues = converted(generation(**restored(**values)))
        self.assertEqual(attrs["gen_ai.output.messages"][0]["parts"], [
            {"type": "tool_call", "id": "call-2", "name": "lookup", "arguments": exact},
            {"type": "custom_part", "payload": [str(big)]}])
        self.assertEqual(attrs["gen_ai.input.messages"][0]["parts"][0]["response"], {"rows": [[str(big), str(low)]]})
        self.assertEqual(issues, [])
        self.assertIn(values["gen_ai.output.messages"], attrs["trace_export.langfuse.row"]["metadata_values"])

        # The fallback never makes an invalid schema field valid: such values stay raw with a warning.
        for message in ({"role": big, "parts": []}, {"role": "user", "parts": [{"type": "tool_call", "name": big}]},
                        {"role": "user", "parts": [], "name": big}):
            with self.subTest(message=message):
                value = json.dumps([message])
                attrs, issues = converted(generation(**restored(**{"gen_ai.input.messages": value})))
                self.assertEqual(attrs["gen_ai.input.messages"], value)
                self.assertEqual(issues, ["existing_attribute_not_schema_compatible:gen_ai.input.messages",
                                          "output_finish_reason_unavailable"])

    def test_unknown_payload_shapes_stay_raw(self):
        image = [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "https://example.test/a.png"}}]}]
        for payload in (json.dumps({"query": "x"}), json.dumps(image), json.dumps(["no role prefix"])):
            with self.subTest(payload=payload):
                record, issues = convert_row(row(type="GENERATION", input=payload))
                attrs = span_attrs(record)
                self.assertNotIn("gen_ai.input.messages", attrs)
                self.assertEqual(issues, ["payload_unmapped:input"])
                self.assertEqual(attrs["trace_export.langfuse.row"]["input"], payload)
        output = json.dumps({"role": "assistant", "content": 5})
        record, issues = convert_row(row(type="AGENT", name="a", output=output))
        self.assertNotIn("gen_ai.output.messages", span_attrs(record))
        self.assertEqual(issues, ["payload_unmapped:output"])

    def test_embedded_langfuse_fields_preserved_only_in_row(self):
        embedded = dict(prompt_id="prompt-1", prompt_name="greeting", prompt_version=3, user_id="user-1", tags=["a", "b"],
                        completion_start_time="2026-01-02T03:04:05.500000Z", version="v2", release="r1",
                        experiment_id="exp-1", experiment_dataset_id="ds-1", experiment_item_id="item-1",
                        cost_details={"total": "0.000000000001"}, provided_cost_details={"input": "0.5"})
        record, _ = convert_row(generation(**embedded))
        attrs = span_attrs(record)
        preserved = attrs["trace_export.langfuse.row"]
        for key, value in embedded.items():
            self.assertEqual(preserved[key], value)
        self.assertFalse([key for key in attrs if not key.startswith(("gen_ai.", "trace_export."))])

    def test_schema_check_matches_pinned_schemas(self):
        validators = message_validators()
        candidates = [
            [], STRUCTURED_INPUT, STRUCTURED_OUTPUT, [{"role": "custom", "parts": [], "finish_reason": "custom"}],
            [{"role": "user", "parts": [{"type": "custom_part", "anything": 1}], "name": None}],
            [{"role": "assistant", "parts": [{"type": "tool_call", "name": "t", "id": None},
                                             {"type": "tool_call_response", "response": None, "id": "c"},
                                             {"type": "uri", "modality": "image", "uri": "https://example.test"},
                                             {"type": "reasoning", "content": "r"}], "finish_reason": "tool_call"}],
            [{"role": "user"}], [{"parts": []}], [{"role": 1, "parts": []}], [{"role": "user", "parts": "x"}],
            [{"role": "user", "parts": [], "name": 3}], [{"role": "user", "parts": [{"content": "x"}]}],
            [{"role": "user", "parts": [{"type": "text"}]}], [{"role": "user", "parts": [{"type": "tool_call"}]}],
            [{"role": "user", "parts": [{"type": "tool_call", "name": "t", "id": 5}]}],
            [{"role": "user", "parts": [{"type": "tool_call_response"}]}],
            [{"role": "user", "parts": [{"type": "blob", "content": "eA=="}]}],
            [{"role": "assistant", "parts": [], "finish_reason": None}], {"role": "user", "parts": []}, "text", None,
        ]
        for candidate in candidates:
            for output, check in validators.items():
                with self.subTest(candidate=candidate, output=output):
                    if schema_messages(candidate, output):
                        check.validate(candidate)
                    elif check.is_valid(candidate):
                        # Stricter only for known part types whose fields are malformed.
                        parts = [part for message in candidate for part in message["parts"]]
                        self.assertTrue(any(part.get("type") in ("text", "tool_call", "tool_call_response", "blob")
                                            for part in parts))
        self.assertTrue(schema_messages(STRUCTURED_OUTPUT, True))
        self.assertFalse(schema_messages(STRUCTURED_INPUT, True))

    def test_mapped_output_matches_contract_protobuf_and_message_schemas(self):
        from google.protobuf.json_format import ParseDict
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        check, validators = validator(), message_validators()
        rows = [generation(), generation(level="ERROR", status_message="boom", parent_span_id=""),
                row(type="AGENT", name="planner", input="Hi", output="Hello"),
                row(type="TOOL", name="weather", input=json.dumps({"city": "Sofia"}), output=json.dumps([1, 2])),
                generation(**restored(**{"gen_ai.input.messages": json.dumps(STRUCTURED_INPUT),
                                         "gen_ai.output.messages": json.dumps(STRUCTURED_OUTPUT)}))]
        for source in rows:
            record, _ = convert_row(source)
            self.assertEqual(validate_record(record, check), 1)
            attrs = span_attrs(record)
            for output, key in ((False, "gen_ai.input.messages"), (True, "gen_ai.output.messages")):
                if key in attrs:
                    validators[output].validate(attrs[key])
            wire = json.loads(json.dumps(record, allow_nan=False))
            span = span_of(wire)
            for key in ("traceId", "spanId", "parentSpanId"):
                if key in span:
                    span[key] = base64.b64encode(bytes.fromhex(span[key])).decode()
            parsed = ParseDict(wire, ExportTraceServiceRequest()).resource_spans[0].scope_spans[0].spans[0]
            self.assertEqual(parsed.name, span_of(record)["name"])

    def test_input_row_not_mutated_and_no_sentinel_in_issues(self):
        source = generation(input=f"{SENTINEL} {{", **restored(**{"gen_ai.output.messages": SENTINEL}))
        original = copy.deepcopy(source)
        _, issues = convert_row(source)
        self.assertEqual(source, original)
        self.assertNotIn(SENTINEL, json.dumps(issues))


if __name__ == "__main__":
    unittest.main()
