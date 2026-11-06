"""ClickHouse source configuration, Langfuse v4 events table inspection and raw JSONL download."""
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from .otlp import atomic_output

DEFAULT_ENV = Path(__file__).resolve().parents[1] / "clickhouse" / ".env"
DEFAULT_DATABASE = "default"
DEFAULT_TABLE = "events_full"
KEYS = ("CLICKHOUSE_URL", "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DB", "CLICKHOUSE_CA_CERT")

# Column kinds read by the exporter, checked against Langfuse 4.x events_full.
REQUIRED_COLUMNS = {
    "project_id": "string", "trace_id": "string", "span_id": "string",
    "parent_span_id": "string", "start_time": "datetime", "end_time": "datetime",
    "name": "string", "type": "string", "environment": "string",
    "trace_name": "string", "user_id": "string", "session_id": "string",
    "tags": "string_array", "level": "string", "status_message": "string",
    "input": "string", "output": "string",
    "metadata_names": "string_array", "metadata_values": "string_array",
    "event_ts": "datetime", "is_deleted": "uint8",
}
# Enrichment columns: type-checked when present, reported when absent.
OPTIONAL_COLUMNS = {
    "version": "string", "release": "string", "completion_start_time": "datetime",
    "prompt_name": "string", "prompt_version": "uint", "model_id": "string",
    "provided_model_name": "string", "model_parameters": "string",
    "usage_details": "uint_map", "provided_usage_details": "uint_map",
    "cost_details": "decimal_map", "provided_cost_details": "decimal_map",
    "tool_definitions": "string_map", "tool_calls": "string_array",
    "tool_call_names": "string_array", "source": "string",
    "service_name": "string", "service_version": "string",
    "scope_name": "string", "scope_version": "string",
    "telemetry_sdk_language": "string", "telemetry_sdk_name": "string",
    "telemetry_sdk_version": "string",
}
# FINAL deduplicates on the whole sorting key, so every key expression must be stable
# observation identity: the Langfuse v4 key expressions or their plain columns, mapped to
# the identity column each fully determines (None: allowed, determines none). Mutable
# columns such as event_ts or is_deleted would keep superseded and deleted versions apart.
SORTING_EXPRESSIONS = {
    "project_id": "project_id", "trace_id": "trace_id", "xxHash32(trace_id)": "trace_id",
    "span_id": "span_id", "start_time": "start_time", "toStartOfMinute(start_time)": None,
}
SORTING_COLUMNS = ("project_id", "trace_id", "span_id", "start_time")
# Never exported: the hashed ingestion key is a credential, not trace data.
EXCLUDED_COLUMNS = ("ingestion_api_key",)
ORDER_COLUMNS = ("start_time", "project_id", "trace_id", "span_id")
CHUNK_BYTES = 1 << 16
KIND_PATTERNS = {
    "string": r"String",
    "datetime": r"DateTime64\(\d(, *'[^']*')?\)",
    "uint8": r"UInt8",
    "uint": r"UInt(8|16|32|64)",
    "string_array": r"Array\(String\)",
    "string_map": r"Map\(String, *String\)",
    "uint_map": r"Map\(String, *UInt64\)",
    "decimal_map": r"Map\(String, *Decimal\(\d+, *\d+\)\)",
}


def settings(env_file=None, *, url=None, database=None):
    from dotenv import dotenv_values
    path = Path(env_file) if env_file is not None else DEFAULT_ENV
    if env_file is not None and not path.is_file():
        raise ValueError(f"Env file not found: {path}")
    # Do not source shell code or interpolate secrets into other settings.
    values = dict(dotenv_values(path, interpolate=False)) if path.is_file() else {}
    values.update(os.environ)
    if url:
        values["CLICKHOUSE_URL"] = url
    if database:
        values["CLICKHOUSE_DB"] = database
    values = {key: values[key] for key in KEYS if values.get(key) is not None}
    missing = [key for key in ("CLICKHOUSE_URL", "CLICKHOUSE_USER") if not values.get(key)]
    if missing:
        raise ValueError("Missing settings: " + ", ".join(missing))
    values.setdefault("CLICKHOUSE_PASSWORD", "")
    values["CLICKHOUSE_DB"] = identifier(values.get("CLICKHOUSE_DB") or DEFAULT_DATABASE)
    secure = endpoint(values["CLICKHOUSE_URL"])[0]
    ca_cert = values.get("CLICKHOUSE_CA_CERT")
    if ca_cert:
        if not secure:
            raise ValueError("CLICKHOUSE_CA_CERT requires an https CLICKHOUSE_URL")
        if not Path(ca_cert).is_file():
            raise ValueError("CLICKHOUSE_CA_CERT file not found")
    return values


def endpoint(url):
    # Messages never echo the URL: it may embed credentials.
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError:
        raise ValueError("CLICKHOUSE_URL is not a valid URL") from None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("CLICKHOUSE_URL must be an http:// or https:// URL with a host")
    if parts.username is not None or parts.password is not None:
        raise ValueError("Put credentials in CLICKHOUSE_USER and CLICKHOUSE_PASSWORD, not CLICKHOUSE_URL")
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        raise ValueError("CLICKHOUSE_URL must not contain a path, query or fragment")
    secure = parts.scheme == "https"
    return secure, parts.hostname, port or (443 if secure else 80)


def identifier(name):
    if not name or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Use unquoted identifiers containing letters, digits and underscores")
    return name


def redact(message, values=None):
    message = str(message)
    values = values or {}
    password = values.get("CLICKHOUSE_PASSWORD")
    # Known secrets first: URL redaction below would otherwise split a password containing @.
    if password:
        user = values.get("CLICKHOUSE_USER", "")
        basic = base64.b64encode(f"{user}:{password}".encode()).decode()
        for secret in sorted({basic, password}, key=len, reverse=True):
            message = message.replace(secret, "<redacted>")
    # Userinfo runs to the last @ of the authority, so an unknown password with @ is covered too.
    return re.sub(r"(?i)\b([a-z][a-z0-9+.-]*://)[^/?#\s]*@", r"\1<redacted>@", message)


def connect(values, *, timeout=300):
    import clickhouse_connect
    secure, host, port = endpoint(values["CLICKHOUSE_URL"])
    try:
        # verify stays on; an optional CA bundle only extends trusted roots.
        return clickhouse_connect.get_client(
            host=host, port=port, secure=secure, verify=True,
            ca_cert=values.get("CLICKHOUSE_CA_CERT") or None,
            username=values["CLICKHOUSE_USER"], password=values.get("CLICKHOUSE_PASSWORD", ""),
            database=values["CLICKHOUSE_DB"], connect_timeout=10, send_receive_timeout=timeout,
            autogenerate_session_id=False)
    except Exception as exc:
        # Drop the chained driver exception: its text and arguments may hold credentials.
        raise RuntimeError(redact(f"ClickHouse connection failed: {exc}", values)) from None


def engine_arguments(engine_full, engine):
    match = re.match(re.escape(engine) + r"\s*\(", engine_full or "")
    if not match:
        return []
    arguments, depth, quoted, current = [], 0, False, ""
    for char in engine_full[match.end():]:
        if quoted:
            quoted = char != "'"
        elif char == "'":
            quoted = True
        elif char == "(":
            depth += 1
        elif char == ")" and depth == 0:
            break
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            arguments.append(current.strip().strip("`"))
            current = ""
            continue
        current += char
    arguments.append(current.strip().strip("`"))
    return [argument for argument in arguments if argument]


def column_kind_matches(column_type, kind):
    base = re.sub(r"LowCardinality\((.*?)\)", r"\1", column_type)
    # Timestamps and prompt versions may be absent; strings and arrays default to empty.
    if kind in ("datetime", "uint"):
        base = re.sub(r"^Nullable\((.*)\)$", r"\1", base)
    return re.fullmatch(KIND_PATTERNS[kind], base) is not None


def inspect_schema(client, database=DEFAULT_DATABASE, table=DEFAULT_TABLE):
    database, table = identifier(database), identifier(table)
    name = f"{database}.{table}"
    parameters = {"database": database, "table": table}
    rows = client.query(
        "SELECT engine, engine_full, sorting_key, partition_key FROM system.tables "
        "WHERE database = {database:String} AND name = {table:String}", parameters=parameters).result_rows
    if not rows:
        raise ValueError(f"Table {name} was not found or is not visible to this user")
    engine, engine_full, sorting_key, partition_key = rows[0]
    if not engine.endswith("ReplacingMergeTree"):
        raise ValueError(f"{name} uses {engine}; expected Langfuse v4 ReplacingMergeTree(event_ts, is_deleted)")
    arguments = engine_arguments(engine_full, engine)
    if arguments[-2:] != ["event_ts", "is_deleted"]:
        raise ValueError(f"{name} must use ReplacingMergeTree(event_ts, is_deleted) replacement semantics")
    # No supported expression contains a comma, so anything else splits into unsupported pieces.
    expressions = [expression.strip() for expression in sorting_key.split(",")] if sorting_key else []
    unsupported = [expression for expression in expressions if expression not in SORTING_EXPRESSIONS]
    if unsupported:
        raise ValueError(f"{name} sorting key has unsupported expressions {', '.join(unsupported)}; "
                         "FINAL would not keep exactly the latest version of each span")
    identity = {SORTING_EXPRESSIONS[expression] for expression in expressions}
    missing_sort = [column for column in SORTING_COLUMNS if column not in identity]
    if missing_sort:
        raise ValueError(f"{name} sorting key lacks {', '.join(missing_sort)}; FINAL would not deduplicate per span")
    columns = dict(client.query(
        "SELECT name, type FROM system.columns WHERE database = {database:String} AND table = {table:String} "
        "ORDER BY position", parameters=parameters).result_rows)
    problems = [f"{column} missing" for column in REQUIRED_COLUMNS if column not in columns]
    for column, kind in {**REQUIRED_COLUMNS, **OPTIONAL_COLUMNS}.items():
        if column in columns and not column_kind_matches(columns[column], kind):
            problems.append(f"{column} has type {columns[column]}")
    if problems:
        raise ValueError(f"{name} is not a supported Langfuse v4 events table: " + "; ".join(problems))
    return {"database": database, "table": table, "engine": engine,
            "version_column": "event_ts", "is_deleted_column": "is_deleted",
            "sorting_key": sorting_key, "partition_key": partition_key, "columns": columns,
            "missing_optional_columns": [column for column in OPTIONAL_COLUMNS if column not in columns]}


def quote(name):
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def utc_text(value, name):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def export_columns(schema):
    # Explicit names: SELECT * would omit MATERIALIZED cost and length columns.
    return [column for column in schema["columns"] if column not in EXCLUDED_COLUMNS]


def build_query(schema, *, since=None, until=None, project_id=None, limit=None, order_by_time=False):
    table = f"{quote(identifier(schema['database']))}.{quote(identifier(schema['table']))}"
    # FINAL keeps the latest event_ts version per span; WHERE runs after it, so deleted spans stay hidden.
    sql = f"SELECT {', '.join(map(quote, export_columns(schema)))} FROM {table} FINAL WHERE `is_deleted` = 0"
    parameters = {}
    for key, value, operator in (("since", since, ">="), ("until", until, "<")):
        if value is not None:
            parameters[key] = utc_text(value, key)
            sql += f" AND `start_time` {operator} {{{key}:DateTime64(6, 'UTC')}}"
    if since is not None and until is not None and since >= until:
        raise ValueError("since must be earlier than until")
    # No project filter exports every project the user can read.
    if project_id is not None:
        if not isinstance(project_id, str) or not project_id:
            raise ValueError("project_id must be a non-empty string")
        parameters["project_id"] = project_id
        sql += " AND `project_id` = {project_id:String}"
    if order_by_time:
        sql += " ORDER BY " + ", ".join(map(quote, ORDER_COLUMNS))
    if limit is not None:
        sql += f" LIMIT {positive_int(limit, 'limit')}"
    return sql, parameters


def query_settings(timeout=300, batch_size=1000):
    return {
        "max_execution_time": positive_int(timeout, "timeout"), "timeout_overflow_mode": "throw",
        # A server block-size hint only; the export is still one query without OFFSET pagination.
        "max_block_size": positive_int(batch_size, "batch_size"),
        "output_format_json_quote_64bit_integers": 0, "output_format_json_quote_decimals": 1,
        "date_time_output_format": "iso", "output_format_json_validate_utf8": 1,
        "optimize_move_to_prewhere_if_final": 0,
    }


PREFLIGHT_SQL = (
    "SELECT toDateTime64('2001-02-03 13:05:06.789012', 6, 'Asia/Tokyo') AS time, "
    "toUInt64('18446744073709551615') AS uint, toInt64('-9223372036854775808') AS int, "
    "toDecimal64('-1.000000000001', 12) AS decimal, map('k', toUInt64('18446744073709551615')) AS uints, "
    "map('k', toDecimal64('0.000000000001', 12)) AS decimals, concat('a', unhex('FF')) AS utf8, "
    "toString(getSetting('max_execution_time')) AS max_execution_time, "
    "toString(getSetting('timeout_overflow_mode')) AS timeout_overflow_mode")
PREFLIGHT_EXPECTED = {
    "time": "2001-02-03T04:05:06.789012Z", "uint": 18446744073709551615, "int": -9223372036854775808,
    "decimal": "-1.000000000001", "uints": {"k": 18446744073709551615}, "decimals": {"k": "0.000000000001"},
    "utf8": "a\ufffd", "timeout_overflow_mode": "throw",
}


def check_settings(client, settings):
    # A constant query proves the server applied every format and timeout setting before the data query runs.
    body = client.raw_query(PREFLIGHT_SQL, settings=settings, fmt="JSONEachRow")
    try:
        row = json.loads(body.decode("utf-8"))
        applied = {key: row[key] for key in PREFLIGHT_EXPECTED}
        timeout = float(row["max_execution_time"])
    except (UnicodeDecodeError, ValueError, TypeError, KeyError):
        raise ValueError("ClickHouse returned an unexpected settings check response") from None
    wrong = sorted(key for key, value in PREFLIGHT_EXPECTED.items() if applied[key] != value)
    if timeout != settings["max_execution_time"]:
        wrong.append("max_execution_time")
    if wrong:
        raise ValueError("ClickHouse did not apply the required export settings (" + ", ".join(wrong) + "); "
                         "the user may be readonly=1 or a settings profile may constrain them")


def stream_error(data, stream):
    # Errors after HTTP 200 arrive in-band: "Code: ..." text, often framed by __exception__ lines,
    # or an {"exception": ...} row. Only those are reported; trace rows are never echoed.
    tail, size = [data], 0
    while size < CHUNK_BYTES and (chunk := stream.read(CHUNK_BYTES - size)):
        tail.append(chunk)
        size += len(chunk)
    for line in b"".join(tail).split(b"\n"):
        text = line.decode("utf-8", "replace")
        if re.match(r"Code: \d+\.", text):
            return text.strip()[:1000]
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict) and set(row) == {"exception"}:
            return str(row["exception"]).strip()[:1000]
    return None


def row_text(line, columns):
    try:
        text = line.decode("utf-8")
        row = json.loads(text, parse_float=str)
    except ValueError:
        return None
    return text if isinstance(row, dict) and set(row) == columns else None


def stream_failure(data, stream, count):
    error = stream_error(data, stream)
    if error:
        return RuntimeError("ClickHouse failed after streaming started: " + error)
    return ValueError(f"ClickHouse returned a malformed or truncated row after {count} rows; nothing was written")


def download(client, schema, output, *, since=None, until=None, project_id=None, limit=None,
             order_by_time=False, timeout=300, batch_size=1000, overwrite=False):
    sql, parameters = build_query(schema, since=since, until=until, project_id=project_id,
                                  limit=limit, order_by_time=order_by_time)
    settings = query_settings(timeout, batch_size)
    columns = set(export_columns(schema))
    count = 0
    with atomic_output(output, overwrite) as handle:
        check_settings(client, settings)
        stream = client.raw_stream(sql, parameters=parameters, settings=settings, fmt="JSONEachRow")
        try:
            # Memory holds one chunk and the current row; b"\n" never occurs inside a UTF-8 sequence.
            pending = []
            while chunk := stream.read(CHUNK_BYTES):
                pieces = chunk.split(b"\n")
                for index, piece in enumerate(pieces[:-1]):
                    line = b"".join(pending + [piece])
                    pending = []
                    text = row_text(line, columns)
                    if text is None:
                        raise stream_failure(b"\n".join([line, *pieces[index + 1:]]), stream, count)
                    # Verbatim server text: 64-bit integers and decimals are never reserialized.
                    handle.write(text + "\n")
                    count += 1
                if pieces[-1]:
                    pending.append(pieces[-1])
            if pending:
                raise stream_failure(b"".join(pending), stream, count)
        finally:
            stream.close()
    return count
