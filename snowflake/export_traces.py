#!/usr/bin/env python3
"""Export Cortex Agent observability rows to a local JSONL file."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trace_export.reporting import export_summary
from trace_export.export_options import add_options, resolve_options, describe_export, emit_output
from trace_export.redaction import export_warnings


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use an ISO timestamp with a timezone") from exc
    if result.tzinfo is None:
        raise argparse.ArgumentTypeError("Timestamp must include Z or a timezone offset")
    return result.astimezone(timezone.utc)


def positive_int(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero")
    return result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--connection", help="Python connector connections.toml profile (otherwise default)")
    result.add_argument("--database", help="Agent database, exact stored name (for an unqualified --agent)")
    result.add_argument("--schema", help="Agent schema, exact stored name (for an unqualified --agent)")
    result.add_argument("--agent", required=True, help="Exact agent name, or DATABASE.SCHEMA.AGENT without separate database/schema")
    result.add_argument("--since", type=timestamp, help="Inclusive time lower bound")
    result.add_argument("--until", type=timestamp, help="Exclusive time upper bound")
    result.add_argument("--all-events", action="store_true", help="Include all record types (default: SPAN only)")
    result.add_argument("--time-column", choices=("START_TIMESTAMP", "TIMESTAMP"),
                        default="START_TIMESTAMP", help="Time filter/order column (default: START_TIMESTAMP)")
    result.add_argument("--limit", type=positive_int, help="Maximum rows (default: 10 in sample mode; may split a trace)")
    result.add_argument("--batch-size", type=positive_int, default=1000)
    result.add_argument("--output", type=Path, help="Destination JSONL file (default: stdout in sample mode)")
    result.add_argument("--format", choices=("otel", "raw"), default="otel", help="Output format (default: GenAI OTLP)")
    result.add_argument("--keep-raw", type=Path, help="Retain the raw download at this path before conversion")
    result.add_argument("--input-token-mode", choices=("auto", "includes-cache", "excludes-cache"), default="auto")
    result.add_argument("--overwrite", action="store_true", help="Replace an existing output after success")
    result.add_argument("--json-summary", action="store_true", help="Print an export summary as JSON on stdout")
    add_options(result)
    return result


def agent_parts(args):
    if args.database is not None or args.schema is not None:
        if not args.database or not args.schema or not args.agent:
            raise ValueError("Provide both --database and --schema, or use --agent DATABASE.SCHEMA.AGENT alone")
        # Preserve the existing literal-name behavior, including dots in names.
        return args.database, args.schema, args.agent
    parts = args.agent.split(".")
    if len(parts) != 3 or any(not part.strip() or '"' in part for part in parts):
        raise ValueError("Use --agent DATABASE.SCHEMA.AGENT, or supply --database and --schema separately; use separate options for names containing dots or SQL quotes")
    return tuple(parts)


def build_query(args):
    sql = """SELECT TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(e.*)) AS ROW_JSON
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    %s, %s, %s, 'CORTEX AGENT'
)) AS e"""
    parameters = list(agent_parts(args))
    predicates = [] if args.all_events else ["e.RECORD_TYPE = 'SPAN'"]
    # Only the CLI allowlist may supply a SQL identifier.
    if args.time_column not in ("START_TIMESTAMP", "TIMESTAMP"):
        raise ValueError("Unsupported time column")
    time_column = f'e."{args.time_column}"'
    for value, operator in ((args.since, ">="), (args.until, "<")):
        if value is not None:
            predicates.append(f'{time_column} {operator} TO_TIMESTAMP_TZ(%s)')
            parameters.append(value.isoformat())
    if predicates:
        sql += "\nWHERE " + " AND ".join(predicates)
    if args.order_by_time:
        sql += f'\nORDER BY {time_column}'
    if args.limit is not None:
        sql += "\nLIMIT %s"
        parameters.append(args.limit)
    return sql, tuple(parameters)


def write_rows(cursor, output, batch_size=1000, overwrite=False):
    """Publish only complete exports; never replace an existing file by default."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not overwrite and output.exists():
        raise FileExistsError(f"Output exists: {output}; use --overwrite to replace it")
    count = 0
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=output.parent,
            prefix=f".{output.name}.", suffix=".partial", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            while batch := cursor.fetchmany(batch_size):
                for (payload,) in batch:
                    # Validate without rounding numbers; write Snowflake's JSON directly.
                    if not isinstance(json.loads(payload, parse_float=str), dict):
                        raise ValueError("Expected a JSON object for each database row")
                    handle.write(payload.replace("\r", "").replace("\n", "").strip() + "\n")
                    count += 1
            handle.flush()
            os.fsync(handle.fileno())
        if overwrite:
            os.replace(temporary, output)
        else:
            # Atomic exclusive publication, including concurrent invocations.
            os.link(temporary, output)
        return count
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    try:
        agent_parts(args)
    except ValueError as exc:
        cli.error(str(exc))
    resolve_options(args, cli)
    if args.output and args.output.exists() and not args.overwrite:
        cli.error("Output already exists; choose another path or use --overwrite")
    if args.keep_raw:
        if args.format == "raw":
            cli.error("--keep-raw requires --format otel; --format raw already retains raw output")
        if args.output and (args.keep_raw.resolve() == args.output.resolve() or (args.keep_raw.exists() and args.output.exists() and args.keep_raw.samefile(args.output))):
            cli.error("--keep-raw must differ from --output")
        if args.keep_raw.exists() and not args.overwrite:
            cli.error("Raw output already exists; choose another path or use --overwrite")
    try:
        import snowflake.connector
    except ImportError:
        requirements = Path(__file__).resolve().with_name("requirements.txt")
        venv = requirements.parent.parent / ".venv"
        cli.exit(1, "Snowflake connector could not be imported.\n"
                 "Recommended: create a virtual environment to keep dependencies isolated.\n"
                 "Run these commands (skip creation if the environment already exists):\n\n"
                 f"  python3 -m venv {shlex.quote(str(venv))}\n"
                 f"  source {shlex.quote(str(venv / 'bin' / 'activate'))}\n"
                 f"  python -m pip install -r {shlex.quote(str(requirements))}\n\n"
                 "Then rerun your export command in the same terminal.\n")
    count = None
    args.export_warnings = []
    describe_export(args)
    if args.sample:
        print("Snowflake row samples may contain incomplete traces.", file=sys.stderr)
    try:
        options = {"connection_name": args.connection} if args.connection else {}
        with tempfile.TemporaryDirectory(prefix="cortex-export-") as temporary:
            output = args.output or Path(temporary) / "output.jsonl"
            raw_output = output if args.format == "raw" else args.keep_raw or Path(temporary) / "raw.jsonl"
            with snowflake.connector.connect(**options) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {args.query_timeout}")
                    cursor.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
                    sql, parameters = build_query(args)
                    cursor.execute(sql, parameters)
                    count = write_rows(cursor, raw_output, args.batch_size, args.overwrite)
                    print(f"Downloaded {count} rows (query {cursor.sfqid})", file=sys.stderr)
            args.export_warnings = export_warnings(raw_output, count)
            for warning in args.export_warnings:
                print(f"Warning [{warning['code']}]: {warning['message']}", file=sys.stderr)
            if args.format == "otel":
                sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
                from trace_export.convert import main as convert_main
                conversion_args = ["--input", str(raw_output), "--output", str(output),
                                   "--input-token-mode", args.input_token_mode]
                if args.overwrite:
                    conversion_args.append("--overwrite")
                result = convert_main(conversion_args)
                if result == 0:
                    emit_output(args, output)
                if args.keep_raw:
                    print(f"Raw download retained at {args.keep_raw}", file=sys.stderr)
                return export_summary("snowflake", args, result, count)
            emit_output(args, output)
            print(f"Raw export written to {args.output or 'stdout'}", file=sys.stderr)
    except Exception as exc:
        print(f"Export failed: {exc}", file=sys.stderr)
        return export_summary("snowflake", args, 1, count)
    return export_summary("snowflake", args, 0, count)


if __name__ == "__main__":
    raise SystemExit(main())
