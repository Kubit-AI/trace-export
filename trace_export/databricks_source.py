"""Databricks SQL source configuration and JSON row export."""
from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from .otlp import atomic_output

DEFAULT_ENV = Path(__file__).resolve().parents[1] / "databricks" / ".env"


def settings(env_file=DEFAULT_ENV, *, connection=None, http_path=None):
    if connection:
        from databricks.sdk.core import Config
        config = Config(profile=connection)
        path = http_path or (f"/sql/1.0/warehouses/{config.warehouse_id}" if config.warehouse_id else None)
        if not path:
            raise ValueError("Set warehouse_id in the Databricks profile or pass --http-path")
        hostname = urlsplit(config.host or "").hostname
        if not hostname:
            raise ValueError("Set host to your workspace URL in the Databricks profile")
        return {"DATABRICKS_SERVER_HOSTNAME": hostname,
                "DATABRICKS_HTTP_PATH": path, "sdk_config": config}
    from dotenv import dotenv_values
    # Do not source shell code or interpolate secrets into other settings.
    values = dict(dotenv_values(env_file, interpolate=False)) if env_file.exists() else {}
    values.update(os.environ)
    if http_path:
        values["DATABRICKS_HTTP_PATH"] = http_path
    required = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise ValueError("Missing settings: " + ", ".join(missing))
    return values


def connect(values):
    from databricks import sql
    config = values.get("sdk_config")
    # Return the callable, not its current headers: OAuth credentials can refresh.
    auth = {"credentials_provider": lambda: config.authenticate} if config else {
        "access_token": values["DATABRICKS_TOKEN"]}
    return sql.connect(server_hostname=values["DATABRICKS_SERVER_HOSTNAME"],
                       http_path=values["DATABRICKS_HTTP_PATH"],
                       **auth,
                       _socket_timeout=60, _retry_stop_after_attempts_count=2)


def quote(name):
    if not name or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Use unquoted identifiers containing letters, digits and underscores")
    return f"`{name}`"


def table_parts(agent, table, catalog, schema):
    name = table or agent
    if not name:
        raise ValueError("Provide --agent or --table")
    pieces = name.split(".")
    if len(pieces) == 3:
        catalog, schema, name = pieces
    elif len(pieces) != 1 or not catalog or not schema:
        raise ValueError("Use CATALOG.SCHEMA.NAME or set catalog and schema")
    if agent and not name.endswith("_trace_unified"):
        name += "_trace_unified"
    result = (catalog, schema, name)
    for part in result:
        quote(part)
    return result


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use an ISO timestamp with Z or a timezone offset") from exc


def positive_int(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("Must be greater than zero")
    return value


def query(parts, since=None, until=None, limit=None, order_by_time=False):
    # Server-side JSON retains nested VARIANTs, nulls and integer precision.
    sql = "SELECT to_json(struct(t.*), map('ignoreNullFields', 'false')) AS row_json FROM "
    sql += ".".join(quote(p) for p in parts) + " AS t"
    predicates, parameters = [], []
    for value, operator in ((since, ">="), (until, "<")):
        if value is not None:
            predicates.append(f"t.request_time {operator} CAST(? AS TIMESTAMP)")
            parameters.append(value.isoformat())
    if predicates:
        sql += " WHERE " + " AND ".join(predicates)
    if order_by_time:
        sql += " ORDER BY t.request_time, t.trace_id"
    if limit is not None:
        sql += " LIMIT ?"
        parameters.append(limit)
    return sql, parameters


def download(cursor, output, batch_size=1000, overwrite=False):
    count = 0
    with atomic_output(output, overwrite) as handle:
        while batch := cursor.fetchmany(batch_size):
            for (payload,) in batch:
                if not isinstance(json.loads(payload, parse_float=str), dict):
                    raise ValueError("Expected one JSON object per database row")
                handle.write(payload.replace("\r", "").replace("\n", "").strip() + "\n")
                count += 1
    return count
