#!/usr/bin/env python3
"""Export Pydantic Logfire spans (the `records` table) to GenAI OTLP JSONL.

Rows are fetched through the Logfire query API with keyset pagination ordered by
(start_timestamp, trace_id, span_id), so no page or time-window cap can drop
rows. Failed requests are retried with backoff; timeouts and server errors also
shrink the page. With --keep-raw and --resume, an interrupted export continues
from the last row saved in the raw file.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import random
import re
import shlex
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trace_export.export_options import add_options, describe_export, emit_output, resolve_options
from trace_export.otlp import unix_nano
from trace_export.reporting import export_summary

TOKEN_KEY = "LOGFIRE_READ_TOKEN"
BASE_URL_KEY = "LOGFIRE_BASE_URL"
DEFAULT_ENV = Path(__file__).with_name(".env")
MAX_PAGE = 10_000  # Logfire query API row cap per request.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
STATE_VERSION = 1
HEX = re.compile("[0-9a-f]+")


# ---------------------------------------------------------------- arguments

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


def page_size(value):
    value = positive_int(value)
    if value > MAX_PAGE:
        raise argparse.ArgumentTypeError(f"Logfire returns at most {MAX_PAGE} rows per request")
    return value


def parser():
    cli = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    cli.add_argument("--env-file", type=Path,
                     help=f"Env file with {TOKEN_KEY} (default: pydantic-logfire/.env when present)")
    cli.add_argument("--base-url", help=f"Logfire API URL (default: {BASE_URL_KEY}, else inferred from the token region)")
    cli.add_argument("--environment", action="append",
                     help="Only rows with this deployment environment; repeatable ('' selects rows without one)")
    cli.add_argument("--since", type=timestamp, help="Inclusive span start_timestamp lower bound")
    cli.add_argument("--until", type=timestamp, help="Exclusive span start_timestamp upper bound")
    cli.add_argument("--limit", type=positive_int,
                     help="Maximum span rows (default: 10 in sample mode; may split a trace)")
    cli.add_argument("--page-size", type=page_size, default=1000,
                     help=f"Rows per API request, at most {MAX_PAGE} (default: 1000; shrinks on timeouts)")
    cli.add_argument("--max-retries", type=int, default=8,
                     help="Consecutive failed attempts per request before giving up (default: 8)")
    cli.add_argument("--output", type=Path, help="Destination JSONL file (default: stdout in sample mode)")
    cli.add_argument("--format", choices=("otel", "raw"), default="otel",
                     help="Output format (default: GenAI OTLP; raw keeps every records column)")
    cli.add_argument("--keep-raw", type=Path, help="Retain the raw download at this path before conversion")
    cli.add_argument("--resume", action="store_true",
                     help="Continue an interrupted download in the --keep-raw file (or --output with --format raw)")
    cli.add_argument("--from-raw", type=Path, metavar="PATH",
                     help="Convert a saved raw export to OTLP locally; no API access")
    cli.add_argument("--overwrite", action="store_true", help="Replace an existing output after success")
    cli.add_argument("--json-summary", action="store_true", help="Print an export summary as JSON on stdout")
    add_options(cli)
    for action in cli._actions:
        if action.dest == "query_timeout":
            action.help = "Per-request HTTP timeout (default: 300 seconds)"
        elif action.dest == "order_by_time":
            action.help = "Accepted for compatibility; results are always in start_timestamp order"
    return cli


# ---------------------------------------------------------------- settings

def settings(env_file=None, base_url=None):
    path = Path(env_file) if env_file is not None else DEFAULT_ENV
    if env_file is not None and not path.is_file():
        raise ValueError(f"Env file not found: {path}")
    values = {}
    if path.is_file():
        from dotenv import dotenv_values
        # Parsed, never sourced; no interpolation of other settings.
        values = dict(dotenv_values(path, interpolate=False))
    values.update(os.environ)
    token = (values.get(TOKEN_KEY) or "").strip()
    if not token:
        raise ValueError(f"Missing {TOKEN_KEY}: set it in the environment or an env file")
    return {"token": token, "base_url": base_url or values.get(BASE_URL_KEY) or None}


def redact(message, token):
    message = str(message)
    if token:
        message = message.replace(token, "[REDACTED]")
    return re.sub(r"pylf_v\d+_[A-Za-z0-9_-]+", "[REDACTED]", message)


# ---------------------------------------------------------------- query

def iso(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def row_key(row):
    """Sort key matching the SQL ORDER BY; validates values before they are embedded in SQL."""
    trace_id, span_id, start = row.get("trace_id"), row.get("span_id"), row.get("start_timestamp")
    if not (isinstance(trace_id, str) and HEX.fullmatch(trace_id) and isinstance(span_id, str) and HEX.fullmatch(span_id)):
        raise ValueError("row has a non-hexadecimal trace or span ID; cannot paginate safely")
    return int(unix_nano(start)), trace_id, span_id, start


def build_sql(since, until, cursor, limit):
    where = ["kind != 'pending_span'"]
    if since:
        where.append(f"start_timestamp >= TIMESTAMP '{iso(since)}'")
    if until:
        where.append(f"start_timestamp < TIMESTAMP '{iso(until)}'")
    if cursor:
        _, trace_id, span_id, start = cursor
        ts = f"TIMESTAMP '{start}'"
        where.append(f"(start_timestamp > {ts} OR (start_timestamp = {ts} AND "
                     f"(trace_id > '{trace_id}' OR (trace_id = '{trace_id}' AND span_id > '{span_id}'))))")
    return ("SELECT * FROM records WHERE " + " AND ".join(where) +
            f" ORDER BY start_timestamp, trace_id, span_id LIMIT {limit}")


def failure(exc):
    """(retryable, shrink page, server-requested delay) for a request error."""
    import httpx
    from logfire.query_client import QueryRateLimitedError, UnexpectedResponseError
    if isinstance(exc, QueryRateLimitedError):
        return True, False, getattr(exc, "retry_after", None)
    if isinstance(exc, UnexpectedResponseError):
        match = re.search(r"status code (\d+)", str(exc))
        status = int(match[1]) if match else 0
        retryable = getattr(exc, "retryable", None)
        retryable = retryable if retryable is not None else (status >= 500 or status in (408, 413))
        return retryable, retryable, getattr(exc, "retry_after", None)
    if isinstance(exc, (httpx.TimeoutException, httpx.RemoteProtocolError)):
        return True, True, None
    if isinstance(exc, httpx.TransportError):
        return True, False, None
    if isinstance(exc, ValueError):  # Truncated or malformed response body.
        return True, True, None
    return False, False, None


def call(action, args, label, size=None, sleep=None):
    """Run action(size) with retries; returns (result, page size that succeeded)."""
    attempts = 0
    while True:
        try:
            return action(size), size
        except Exception as exc:
            retryable, shrink, delay = failure(exc)
            attempts += 1
            if not retryable or attempts > args.max_retries:
                raise
            shrink = shrink and size is not None and size > 1
            if shrink:
                size = max(1, size // 2)
            delay = delay if delay is not None else min(60.0, 2 ** attempts) * (0.5 + random.random() / 2)
            print(f"{label} failed ({type(exc).__name__}); retry {attempts}/{args.max_retries} in {delay:.1f}s"
                  + (f" with page size {size}" if shrink else ""), file=sys.stderr)
            (sleep or time.sleep)(delay)


def download(client, args, raw, cursor=None, count=0, sleep=None):
    """Append ordered rows to raw (an open text file); returns the total row count."""
    lower = args.since or EPOCH
    size = args.page_size
    args.downloaded = count
    while args.limit is None or count < args.limit:
        def fetch(current):
            want = current if args.limit is None else min(current, args.limit - count)
            result = client.query_json_rows(
                sql=build_sql(args.since, args.until, cursor, want),
                # The API bounds are inclusive and only prune storage; SQL enforces exact bounds.
                min_timestamp=datetime.fromisoformat(cursor[3].replace("Z", "+00:00")) if cursor else lower,
                max_timestamp=args.until, limit=want, environment=args.environment)
            rows = result["rows"]
            if len(rows) > want:
                raise RuntimeError("Logfire returned more rows than requested")
            return rows
        rows, size = call(fetch, args, "Logfire query", size, sleep)
        if not rows:
            break
        for row in rows:
            key = row_key(row)
            if cursor and key[:3] <= cursor[:3]:
                raise RuntimeError("Logfire rows arrived out of order; stopping to avoid gaps or duplicates")
            raw.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            cursor = key
        raw.flush()
        os.fsync(raw.fileno())
        count += len(rows)
        args.downloaded = count
        print(f"Downloaded {count} rows; last start_timestamp {cursor[3]}", file=sys.stderr)
        # Recover toward the requested page size after a shrink.
        size = min(args.page_size, size * 2)
    return count


# ---------------------------------------------------------------- resume

def state_path(raw):
    return raw.with_name(raw.name + ".state.json")


def export_state(args):
    return {"version": STATE_VERSION, "since": args.since.isoformat() if args.since else None,
            "until": args.until.isoformat() if args.until else None, "environment": args.environment}


def resume_point(raw, args):
    """Truncate a partial final line; return (cursor, existing row count)."""
    state = state_path(raw)
    if not state.is_file():
        raise ValueError(f"Cannot resume: {state} not found")
    if json.loads(state.read_text()) != export_state(args):
        raise ValueError("Cannot resume: --since, --until and --environment must match the interrupted export")
    with raw.open("rb+") as handle:
        data = handle.read()
        end = data.rfind(b"\n") + 1
        if end < len(data):
            handle.truncate(end)
            print("Removed a partial final line from the raw file", file=sys.stderr)
    lines = data[:end].splitlines()
    if not lines:
        return None, 0
    return row_key(json.loads(lines[-1])), len(lines)


# ---------------------------------------------------------------- main

def convert(raw, output, overwrite):
    from trace_export.convert import main as convert_main
    options = ["--source", "logfire", "--input", str(raw), "--output", str(output)]
    return convert_main(options + (["--overwrite"] if overwrite else []))


def check_args(cli, args):
    if args.from_raw:
        if not args.output:
            cli.error("--from-raw requires --output")
        if args.format == "raw" or args.keep_raw or args.resume:
            cli.error("--from-raw cannot be combined with --format raw, --keep-raw or --resume")
        if args.output.exists() and not args.overwrite:
            cli.error(f"Output exists: {args.output}; choose another path or use --overwrite")
        return
    resolve_options(args, cli)
    args.order_by_time = True  # Keyset pagination always sorts.
    if args.max_retries < 0:
        cli.error("--max-retries must be zero or more")
    if args.keep_raw and args.format == "raw":
        cli.error("--keep-raw requires --format otel; --format raw already retains raw output")
    if args.keep_raw and args.output and (args.keep_raw.resolve() == args.output.resolve()):
        cli.error("--keep-raw must differ from --output")
    args.raw_target = args.output if args.format == "raw" else args.keep_raw
    if args.resume:
        if not args.raw_target:
            cli.error("--resume requires --keep-raw (or --output with --format raw)")
        if args.overwrite and args.format == "raw":
            cli.error("--resume cannot be combined with --overwrite for --format raw")
        if not args.raw_target.exists():
            cli.error(f"Cannot resume: {args.raw_target} not found")
    for path in (args.output, args.keep_raw):
        if path and path.exists() and not args.overwrite and not (args.resume and path == args.raw_target):
            cli.error(f"Output exists: {path}; choose another path or use --overwrite")


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    check_args(cli, args)
    if args.from_raw:
        return convert(args.from_raw, args.output, args.overwrite)
    values, count = {}, None
    args.downloaded = None
    try:
        values = settings(args.env_file, args.base_url)
        from httpx import Timeout
        from logfire.query_client import LogfireQueryClient
        describe_export(args)
        if args.sample:
            print("Logfire span samples may contain incomplete traces.", file=sys.stderr)
        with tempfile.TemporaryDirectory(prefix="logfire-export-") as temporary, \
                LogfireQueryClient(read_token=values["token"], base_url=values["base_url"],
                                   timeout=Timeout(args.query_timeout, connect=30)) as client:
            info, _ = call(lambda _: client.info(), args, "Read token check")
            print(f"Logfire project: {info.get('organization_name')}/{info.get('project_name')} "
                  f"({client.base_url})", file=sys.stderr)
            output = args.output or Path(temporary) / "output.jsonl"
            raw = args.raw_target or (output if args.format == "raw" else Path(temporary) / "raw.jsonl")
            cursor, count = None, 0
            if args.resume:
                cursor, count = resume_point(raw, args)
                print(f"Resuming after {count} saved rows", file=sys.stderr)
            elif args.raw_target:
                raw.parent.mkdir(parents=True, exist_ok=True)
                raw.write_text("")
            if args.raw_target:
                state_path(raw).write_text(json.dumps(export_state(args)))
            with raw.open("a", encoding="utf-8") as handle:
                count = download(client, args, handle, cursor, count)
            if args.raw_target:
                state_path(raw).unlink(missing_ok=True)
            print(f"Downloaded {count} span rows in total", file=sys.stderr)
            if args.format == "raw":
                emit_output(args, output)
                print(f"Raw export written to {args.output or 'stdout'}", file=sys.stderr)
                return export_summary("logfire", args, 0, count)
            result = convert(raw, output, args.overwrite)
            if result == 0:
                emit_output(args, output)
            if args.keep_raw:
                print(f"Raw download retained at {args.keep_raw}", file=sys.stderr)
            return export_summary("logfire", args, result, count)
    except ImportError as exc:
        root = Path(__file__).resolve().parents[1]
        cli.exit(1, f"Missing module {exc.name or exc}. Install dependencies in a virtual environment (skip creation if it exists):\n"
                 f"  python3 -m venv {shlex.quote(str(root / '.venv'))}\n"
                 f"  source {shlex.quote(str(root / '.venv/bin/activate'))}\n"
                 f"  python -m pip install -r {shlex.quote(str(Path(__file__).with_name('requirements.txt')))}\n")
    except KeyboardInterrupt:
        hint = " Rerun with --resume to continue." if args.raw_target else ""
        print(f"Interrupted after {args.downloaded or 0} saved rows.{hint}", file=sys.stderr)
        return 130
    except Exception as exc:
        hint = " Rerun with --resume to continue." if args.raw_target and args.downloaded else ""
        print(f"Export failed: {redact(exc, values.get('token')) or type(exc).__name__}.{hint}", file=sys.stderr)
        return export_summary("logfire", args, 1, args.downloaded)


if __name__ == "__main__":
    raise SystemExit(main())
