import base64
import copy
from contextlib import redirect_stderr
from datetime import datetime, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from trace_export.databricks_source import download, query, settings, table_parts
from trace_export.mlflow import convert_row, trace_id
from trace_export.convert import main as convert_main

ROOT = Path(__file__).resolve().parents[2]


def span_list(envelope):
    return envelope['resourceSpans'][0]['scopeSpans'][0]['spans']


def unpack(value):
    if 'arrayValue' in value:
        return [unpack(v) for v in value['arrayValue']['values']]
    if 'kvlistValue' in value:
        return attrs(value['kvlistValue']['values'])
    if 'intValue' in value:
        return int(value['intValue'])
    return next(iter(value.values()), None)


def attrs(values):
    return {v['key']: unpack(v['value']) for v in values}


class DatabricksTests(unittest.TestCase):
    def setUp(self):
        self.row = json.loads((Path(__file__).parent/'fixtures/unified_row.json').read_text())

    def test_names_and_parameterized_time_filters(self):
        self.assertEqual(table_parts('main.demo.agent',None,None,None), ('main','demo','agent_trace_unified'))
        self.assertEqual(table_parts('agent_trace_unified',None,'main','demo'), ('main','demo','agent_trace_unified'))
        self.assertEqual(table_parts(None,'main.demo.view',None,None), ('main','demo','view'))
        with self.assertRaises(ValueError):
            table_parts(None,'main.demo.view;DROP',None,None)
        sql, params = query(('main','demo','view'),datetime(2026,1,1,tzinfo=timezone.utc),limit=20)
        self.assertIn('`main`.`demo`.`view`',sql)
        self.assertIn('ignoreNullFields',sql)
        self.assertIn('CAST(? AS TIMESTAMP)',sql)
        self.assertNotIn('2026',sql)
        self.assertEqual(params,['2026-01-01T00:00:00+00:00',20])

    def test_env_file_loading_and_environment_override(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ,{},clear=True):
            env=Path(folder)/'.env'
            env.write_text('DATABRICKS_SERVER_HOSTNAME=example\nDATABRICKS_HTTP_PATH=/sql/test\nDATABRICKS_TOKEN="dummy"\nDATABRICKS_CATALOG=main\n')
            self.assertEqual(settings(env)['DATABRICKS_TOKEN'],'dummy')
            os.environ['DATABRICKS_TOKEN']='override'
            self.assertEqual(settings(env)['DATABRICKS_TOKEN'],'override')
            env.write_text('DATABRICKS_TOKEN=dummy\n')
            with self.assertRaisesRegex(ValueError,'DATABRICKS_SERVER_HOSTNAME'):
                settings(env)

    def test_preserves_all_spans_and_metadata(self):
        result, _ = convert_row(self.row)
        spans = span_list(result)
        self.assertEqual(len(spans),4)
        self.assertEqual([s['name'] for s in spans],['invoke_agent Demo','chat example-model','execute_tool lookup','parse'])
        self.assertEqual(spans[2]['parentSpanId'],spans[1]['spanId'])
        self.assertEqual(spans[0]['traceId'],self.row['trace_id'][3:])
        self.assertEqual(spans[0]['startTimeUnixNano'],'1790208000123456789')
        scope=result['resourceSpans'][0]['scopeSpans'][0]['scope']
        raw=attrs(scope['attributes'])['trace_export.databricks.row']
        self.assertEqual(raw['trace_metadata'],self.row['trace_metadata'])
        self.assertIsNone(raw['client_request_id'])

    def test_tokens_not_repeated_on_parent_and_tool_payloads(self):
        result, _ = convert_row(self.row)
        root,model,tool,_ = [attrs(s['attributes']) for s in span_list(result)]
        self.assertNotIn('gen_ai.usage.input_tokens',root)
        self.assertEqual(model['gen_ai.usage.input_tokens'],100)
        self.assertEqual(model['gen_ai.usage.output_tokens'],20)
        self.assertEqual(tool['gen_ai.tool.call.arguments'],{'query':'example'})
        self.assertEqual(tool['gen_ai.tool.call.result'],{'found':True})
        self.assertEqual(tool['gen_ai.tool.call.id'],'call-1')

    def test_otlp_wire_and_genai_message_schemas(self):
        from google.protobuf.json_format import ParseDict
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
        from jsonschema import Draft202012Validator
        validators={d:Draft202012Validator(json.loads((ROOT/f'snowflake/tests/fixtures/semconv-v1.39.0/gen-ai-{d}-messages.json').read_text())) for d in ('input','output')}
        result,_=convert_row(self.row)
        for span in span_list(result):
            mapped=attrs(span['attributes'])
            for direction,validator in validators.items():
                if f'gen_ai.{direction}.messages' in mapped:
                    validator.validate(mapped[f'gen_ai.{direction}.messages'])
            for key in ('traceId','spanId','parentSpanId'):
                if key in span:span[key]=base64.b64encode(bytes.fromhex(span[key])).decode()
        request=ParseDict(result,ExportTraceServiceRequest())
        self.assertEqual(len(request.resource_spans[0].scope_spans[0].spans),4)

    def test_v4_trace_id_and_invalid_ids(self):
        self.assertEqual(trace_id('trace:/main.demo.agent/'+self.row['trace_id'][3:]),self.row['trace_id'][3:])
        with self.assertRaises(ValueError):trace_id('not-hex')
        self.row['spans'][1]['trace_id']='tr-'+'f'*32
        with self.assertRaisesRegex(ValueError,'differs'):convert_row(self.row)

    def test_timing_correction_does_not_remove_span(self):
        source=self.row['spans'][2]
        source['end_time_unix_nano']=source['start_time_unix_nano']-100
        result,issues=convert_row(self.row)
        span=span_list(result)[2]
        self.assertEqual(span['startTimeUnixNano'],span['endTimeUnixNano'])
        self.assertIn('span_timing_corrected',issues)
        self.assertEqual(attrs(span['attributes'])['trace_export.databricks.span']['end_time_unix_nano'],source['end_time_unix_nano'])

    def test_observed_agent_and_model_message_wrappers(self):
        agent,model=self.row['spans'][:2]
        agent['attributes']['mlflow.spanInputs']={'input':[{'role':'user','content':'hello'}]}
        agent['attributes']['mlflow.spanOutputs']={'output':[{'role':'assistant','content':[{'type':'output_text','text':'world'}]}]}
        model['attributes']['mlflow.spanOutputs']={'tool_calls':[{'id':'call-1','function':{'name':'lookup','arguments':'{}'}}]}
        result,issues=convert_row(self.row)
        self.assertFalse(any(x.startswith('payload_unmapped') for x in issues))
        a=attrs(span_list(result)[0]['attributes'])
        self.assertEqual(a['gen_ai.output.messages'][0]['parts'],[{'type':'text','content':'world'}])
        b=attrs(span_list(result)[1]['attributes'])
        self.assertEqual(b['gen_ai.output.messages'][0]['parts'][0]['type'],'tool_call')

    def test_resource_scope_events_and_links_preserved(self):
        span=self.row['spans'][0]
        span['resource']={'attributes':{'service.name':'original'},'dropped_attributes_count':2}
        span['instrumentation_scope']={'name':'original-scope','version':'1.2','attributes':{'custom':True}}
        span['events']=[{'time_unix_nano':1790208000123456789,'name':'exception','attributes':{'handled':True}}]
        span['links']=[{'trace_id':'tr-'+'a'*32,'span_id':'b'*16,'attributes':{}}]
        result,_=convert_row(self.row)
        first=result['resourceSpans'][0]
        self.assertEqual(attrs(first['resource']['attributes'])['service.name'],'original')
        scope=first['scopeSpans'][0]['scope']
        self.assertEqual(scope['name'],'original-scope')
        self.assertEqual(scope['version'],'1.2')
        converted=first['scopeSpans'][0]['spans'][0]
        self.assertEqual(converted['events'][0]['name'],'exception')
        self.assertEqual(converted['links'][0]['traceId'],'a'*32)

    def test_no_silent_span_skips(self):
        self.row['spans'][1]['span_id']='invalid'
        with self.assertRaisesRegex(ValueError,'span 2:'):convert_row(self.row)

    def test_empty_trace_row_preserved(self):
        self.row['spans']=[]
        result,_=convert_row(self.row)
        self.assertEqual(span_list(result),[])

    def test_raw_download_is_atomic_and_retains_nulls(self):
        with tempfile.TemporaryDirectory() as folder:
            out=Path(folder)/'raw.jsonl';out.write_text('old')
            cursor=MagicMock();cursor.fetchmany.side_effect=[[('{"value":null}',)],RuntimeError('lost connection')]
            with self.assertRaises(RuntimeError):download(cursor,out,overwrite=True)
            self.assertEqual(out.read_text(),'old')
            cursor.fetchmany.side_effect=[[('{"value":null}',)],[]]
            self.assertEqual(download(cursor,out,overwrite=True),1)
            self.assertEqual(json.loads(out.read_text()),{'value':None})
            self.assertEqual(len(list(Path(folder).iterdir())),1)

    def test_converter_one_line_per_row(self):
        with tempfile.TemporaryDirectory() as folder, redirect_stderr(io.StringIO()) as diagnostic:
            inp,out=Path(folder)/'raw.jsonl',Path(folder)/'otel.jsonl'
            inp.write_text(json.dumps(self.row)+'\n')
            self.assertEqual(convert_main(['--source','databricks','--input',str(inp),'--output',str(out)]),0)
            self.assertEqual(len(out.read_text().splitlines()),1)
            self.assertIn('Converted 4 spans from 1 rows',diagnostic.getvalue())

    def test_export_cleans_temporary_raw_and_keeps_explicit_copy(self):
        spec=importlib.util.spec_from_file_location('dbx_export',ROOT/'databricks/export_traces.py')
        exporter=importlib.util.module_from_spec(spec);spec.loader.exec_module(exporter)
        connection=MagicMock();cursor=connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        original=exporter.download;paths=[]
        def record(cur,path,*args):
            paths.append(path);return original(cur,path,*args)
        with tempfile.TemporaryDirectory() as folder, patch.object(exporter,'settings',return_value={}), patch.object(exporter,'connect',return_value=connection), patch.object(exporter,'download',side_effect=record), redirect_stderr(io.StringIO()):
            out,raw=Path(folder)/'out.jsonl',Path(folder)/'raw.jsonl'
            args=['--table','main.demo.agent_trace_unified','--output',str(out)]
            cursor.fetchmany.side_effect=[[(json.dumps(self.row),)],[]]
            self.assertEqual(exporter.main(args),0)
            self.assertFalse(paths[-1].exists())
            cursor.fetchmany.side_effect=[[(json.dumps(self.row),)],[]]
            self.assertEqual(exporter.main(args+['--overwrite','--keep-raw',str(raw)]),0)
            self.assertTrue(raw.exists())


if __name__=='__main__':unittest.main()
