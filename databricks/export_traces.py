#!/usr/bin/env python3
"""Export a Databricks MLflow unified trace view to GenAI OTLP JSONL."""
import argparse
from pathlib import Path
import shlex
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trace_export.databricks_source import DEFAULT_ENV, connect, download, positive_int, query, quote, settings, table_parts, timestamp
from trace_export.reporting import export_summary
from trace_export.export_options import add_options, resolve_options, describe_export, emit_output


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    auth = cli.add_mutually_exclusive_group()
    auth.add_argument("--connection", help="Named profile in ~/.databrickscfg (recommended)")
    auth.add_argument("--env-file", type=Path, default=DEFAULT_ENV,
                      help="Development .env file; ignored with --connection")
    cli.add_argument("--http-path", help="Override the SQL warehouse HTTP path")
    cli.add_argument("--catalog", help="Defaults to DATABRICKS_CATALOG")
    cli.add_argument("--schema", help="Defaults to DATABRICKS_SCHEMA")
    selection = cli.add_mutually_exclusive_group(required=True)
    selection.add_argument("--agent", help="Agent prefix or CATALOG.SCHEMA.PREFIX; adds _trace_unified")
    selection.add_argument("--table", help="Exact view name or CATALOG.SCHEMA.VIEW")
    selection.add_argument("--list-agents", action="store_true", help="List unified trace views in catalog/schema")
    cli.add_argument("--since", type=timestamp)
    cli.add_argument("--until", type=timestamp)
    cli.add_argument("--limit", type=positive_int, help="Maximum database rows (default: 10 in sample mode)")
    cli.add_argument("--batch-size", type=positive_int, default=1000)
    cli.add_argument("--output", type=Path, help="Destination JSONL file (default: stdout in sample mode)")
    cli.add_argument("--format", choices=("otel", "raw"), default="otel")
    cli.add_argument("--keep-raw", type=Path)
    cli.add_argument("--overwrite", action="store_true")
    cli.add_argument("--json-summary", action="store_true", help="Print an export summary as JSON on stdout")
    add_options(cli)
    return cli


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.list_agents and args.json_summary:
        cli.error("Use discover.py --json for machine-readable discovery")
    if not args.list_agents:
        resolve_options(args, cli)
    if args.keep_raw and (args.format == "raw" or args.list_agents):
        cli.error("--keep-raw requires an OTLP export")
    if args.keep_raw and args.output and (args.keep_raw.resolve() == args.output.resolve() or
                         (args.keep_raw.exists() and args.output.exists() and args.keep_raw.samefile(args.output))):
        cli.error("--keep-raw must differ from --output")
    for path in (args.output, args.keep_raw):
        if path and path.exists() and not args.overwrite:
            cli.error(f"Output exists: {path}; choose another path or use --overwrite")
    values = {}
    count = None
    try:
        values = settings(args.env_file, connection=args.connection, http_path=args.http_path)
        catalog = args.catalog or values.get("DATABRICKS_CATALOG")
        schema = args.schema or values.get("DATABRICKS_SCHEMA")
        if args.list_agents:
            scope = quote(catalog) + "." + quote(schema)
        else:
            parts = table_parts(args.agent, args.table, catalog, schema)
            describe_export(args)
        with tempfile.TemporaryDirectory(prefix="databricks-export-") as temporary:
            output = args.output or Path(temporary) / "output.jsonl"
            with connect(values) as connection, connection.cursor() as cursor:
                cursor.execute(f"SET STATEMENT_TIMEOUT = {args.query_timeout}")
                cursor.execute("SET TIME ZONE 'UTC'")
                if args.list_agents:
                    cursor.execute(f"SHOW TABLES IN {scope}")
                    while batch := cursor.fetchmany(1000):
                        for row in batch:
                            if row[1].endswith("_trace_unified"):
                                print(f"{catalog}.{schema}.{row[1]}")
                    return 0
                raw = output if args.format == "raw" else args.keep_raw or Path(temporary) / "raw.jsonl"
                sql, parameters = query(parts, args.since, args.until, args.limit, args.order_by_time)
                cursor.execute(sql, parameters)
                count = download(cursor, raw, args.batch_size, args.overwrite)
                print(f"Downloaded {count} trace rows from {'.'.join(parts)}", file=sys.stderr)
            if args.format == "raw":
                emit_output(args, output)
                print(f"Raw export written to {args.output or 'stdout'}", file=sys.stderr)
                return export_summary("databricks", args, 0, count)
            from trace_export.convert import main as convert_main
            options = ["--source", "databricks", "--input", str(raw), "--output", str(output)]
            if args.overwrite:
                options.append("--overwrite")
            result = convert_main(options)
            if result == 0:
                emit_output(args, output)
            if args.keep_raw:
                print(f"Raw download retained at {args.keep_raw}", file=sys.stderr)
            return export_summary("databricks", args, result, count)
    except ImportError:
        root = Path(__file__).resolve().parents[1]
        cli.exit(1, "Install dependencies in a virtual environment (skip creation if it exists):\n"
                 f"  python3 -m venv {shlex.quote(str(root / '.venv'))}\n"
                 f"  source {shlex.quote(str(root / '.venv/bin/activate'))}\n"
                 f"  python -m pip install -r {shlex.quote(str(Path(__file__).with_name('requirements.txt')))}\n")
    except Exception as exc:
        message = str(exc)
        config = values.get("sdk_config")
        secrets = [values.get("DATABRICKS_TOKEN")]
        if config:
            secrets.extend(getattr(config, key, None) for key in ("token", "client_secret", "azure_client_secret"))
        for secret in secrets:
            if secret:
                message = message.replace(secret, "<redacted>")
        print(f"Export failed: {message}", file=sys.stderr)
        return export_summary("databricks", args, 1, count)


if __name__ == "__main__":
    raise SystemExit(main())
