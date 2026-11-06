#!/usr/bin/env python3
"""Export Langfuse v4 spans from a ClickHouse events table to GenAI OTLP JSONL."""
import argparse
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
import shlex
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trace_export.clickhouse_source import DEFAULT_TABLE, connect, download, identifier, inspect_schema, redact, settings
from trace_export.reporting import export_summary
from trace_export.export_options import add_options, resolve_options, describe_export, emit_output


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use an ISO timestamp with Z or a timezone offset") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("Timestamp must include Z or a timezone offset")
    return parsed.astimezone(timezone.utc)


def positive_int(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero")
    return value


def name(value):
    try:
        return identifier(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def project(value):
    if not value:
        raise argparse.ArgumentTypeError("Must be a non-empty Langfuse project ID")
    return value


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--env-file", type=Path, help="Development .env file (default: clickhouse/.env when present)")
    cli.add_argument("--url", help="Override CLICKHOUSE_URL (http:// or https://, no credentials or path)")
    cli.add_argument("--database", type=name, help="Override CLICKHOUSE_DB (default: default)")
    cli.add_argument("--table", type=name, default=DEFAULT_TABLE, help=f"Langfuse v4 events table (default: {DEFAULT_TABLE})")
    cli.add_argument("--project-id", type=project,
                     help="Export one Langfuse project (default: every project this user can read)")
    cli.add_argument("--since", type=timestamp, help="Inclusive span start_time lower bound")
    cli.add_argument("--until", type=timestamp, help="Exclusive span start_time upper bound")
    cli.add_argument("--limit", type=positive_int,
                     help="Maximum span rows across all selected projects (default: 10 in sample mode; may split a trace)")
    cli.add_argument("--batch-size", type=positive_int, default=1000,
                     help="Server block-size hint (max_block_size); rows still stream from one query")
    cli.add_argument("--output", type=Path, help="Destination JSONL file (default: stdout in sample mode)")
    cli.add_argument("--format", choices=("otel", "raw"), default="otel",
                     help="Output format (default: GenAI OTLP; raw keeps every observation type as stored)")
    cli.add_argument("--keep-raw", type=Path, help="Retain the raw download at this path before conversion")
    cli.add_argument("--overwrite", action="store_true", help="Replace an existing output after success")
    cli.add_argument("--json-summary", action="store_true", help="Print an export summary as JSON on stdout")
    add_options(cli)
    return cli


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    resolve_options(args, cli)
    if args.keep_raw and args.format == "raw":
        cli.error("--keep-raw requires --format otel; --format raw already retains raw output")
    if args.keep_raw and args.output and (args.keep_raw.resolve() == args.output.resolve() or
                         (args.keep_raw.exists() and args.output.exists() and args.keep_raw.samefile(args.output))):
        cli.error("--keep-raw must differ from --output")
    for path in (args.output, args.keep_raw):
        if path and path.exists() and not args.overwrite:
            cli.error(f"Output exists: {path}; choose another path or use --overwrite")
    values = {}
    count = None
    try:
        # The project filter comes only from --project-id, never from Langfuse init settings.
        values = settings(args.env_file, url=args.url, database=args.database)
        describe_export(args)
        scope = (f"project {args.project_id}" if args.project_id else
                 "all projects readable by this ClickHouse user" + ("; the row limit is global" if args.limit else ""))
        print(f"Project scope: {scope}.", file=sys.stderr)
        if args.sample:
            print("ClickHouse span samples may contain incomplete traces.", file=sys.stderr)
        with tempfile.TemporaryDirectory(prefix="clickhouse-export-") as temporary:
            output = args.output or Path(temporary) / "output.jsonl"
            raw = output if args.format == "raw" else args.keep_raw or Path(temporary) / "raw.jsonl"
            client = connect(values, timeout=args.query_timeout)
            try:
                schema = inspect_schema(client, values["CLICKHOUSE_DB"], args.table)
                if schema["missing_optional_columns"]:
                    print("Optional columns absent (not exported): " + ", ".join(schema["missing_optional_columns"]),
                          file=sys.stderr)
                count = download(client, schema, raw, since=args.since, until=args.until, project_id=args.project_id,
                                 limit=args.limit, order_by_time=args.order_by_time, timeout=args.query_timeout,
                                 batch_size=args.batch_size, overwrite=args.overwrite)
            finally:
                # Closing only releases the connection pool; it must not mask the export result.
                with suppress(Exception):
                    client.close()
            print(f"Downloaded {count} span rows from {schema['database']}.{schema['table']}", file=sys.stderr)
            if args.format == "raw":
                emit_output(args, output)
                print(f"Raw export written to {args.output or 'stdout'}", file=sys.stderr)
                return export_summary("clickhouse", args, 0, count)
            from trace_export.convert import main as convert_main
            options = ["--source", "clickhouse", "--input", str(raw), "--output", str(output)]
            if args.overwrite:
                options.append("--overwrite")
            result = convert_main(options)
            if result == 0:
                emit_output(args, output)
            if args.keep_raw:
                print(f"Raw download retained at {args.keep_raw}", file=sys.stderr)
            return export_summary("clickhouse", args, result, count)
    except ImportError:
        root = Path(__file__).resolve().parents[1]
        cli.exit(1, "Install dependencies in a virtual environment (skip creation if it exists):\n"
                 f"  python3 -m venv {shlex.quote(str(root / '.venv'))}\n"
                 f"  source {shlex.quote(str(root / '.venv/bin/activate'))}\n"
                 f"  python -m pip install -r {shlex.quote(str(Path(__file__).with_name('requirements.txt')))}\n")
    except Exception as exc:
        # Driver and server messages may embed URLs, credentials or auth headers; row content is never included.
        print(f"Export failed: {redact(exc, values) or type(exc).__name__}", file=sys.stderr)
        return export_summary("clickhouse", args, 1, count)


if __name__ == "__main__":
    raise SystemExit(main())
