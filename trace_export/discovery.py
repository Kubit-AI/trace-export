"""Read-only metadata discovery. No trace payloads or credentials in results."""
import argparse
import fnmatch
import json
from pathlib import Path
import sys


def quoted(value, delimiter):
    return delimiter + value.replace(delimiter, delimiter * 2) + delimiter


def rows(cursor, statement):
    cursor.execute(statement)
    names = [column[0].lower() for column in cursor.description]
    result = []
    while batch := cursor.fetchmany(1000):
        result.extend(dict(zip(names, row)) for row in batch)
    return result


def report(source):
    return {"version": "1.0.0", "source": source, "status": "ok", "complete": True,
            "visibility": "current_credentials",
            "visibility_note": "Only objects visible to the current credentials are returned. "
                               "Missing objects cannot be distinguished from objects hidden by permissions.",
            "items": [], "issues": []}


def issue(result, scope, code, exc=None):
    entry = {"scope": scope, "code": code}
    # Connector exception strings can contain credentials or connection details.
    if exc is not None:
        entry["error_type"] = type(exc).__name__
    result["issues"].append(entry)
    result.update(status="partial", complete=False)


def matches(name, pattern):
    return fnmatch.fnmatchcase(name.casefold(), pattern.casefold())


def snowflake_discover(cursor, database=None, schema=None, match="*"):
    result = report("snowflake")
    scope = "ACCOUNT"
    if database:
        scope = "DATABASE " + quoted(database, '"')
    if schema:
        scope = "SCHEMA " + quoted(database, '"') + "." + quoted(schema, '"')
    agents = rows(cursor, "SHOW AGENTS IN " + scope)
    if len(agents) >= 10000:
        issue(result, scope, "metadata_limit_reached_narrow_scope")
    for row in agents:
        if not matches(row["name"], match):
            continue
        db, sch, name = row["database_name"], row["schema_name"], row["name"]
        result["items"].append({"kind": "cortex_agent", "database": db, "schema": sch,
                                "name": name, "qualified_name": ".".join((db, sch, name)),
                                "validation": "metadata_only", "export_args": [
                                    "--database", db, "--schema", sch, "--agent", name]})
    result["items"].sort(key=lambda item: item["qualified_name"])
    return result


def databricks_discover(cursor, catalog=None, schema=None, match="*", verify=True):
    from .databricks_source import quote
    result = report("databricks")
    catalogs = [catalog] if catalog else [row["catalog"] for row in rows(cursor, "SHOW CATALOGS")]
    for cat in sorted(catalogs):
        try:
            schemas = [schema] if schema else [row["databasename"] for row in rows(
                cursor, "SHOW SCHEMAS IN " + quoted(cat, '`'))]
        except Exception as exc:
            issue(result, cat, "scope_query_failed", exc)
            continue
        for sch in sorted(schemas):
            scope = quoted(cat, '`') + "." + quoted(sch, '`')
            try:
                tables = rows(cursor, "SHOW TABLES IN " + scope)
            except Exception as exc:
                issue(result, cat + "." + sch, "scope_query_failed", exc)
                continue
            for table in tables:
                name = table["tablename"]
                if table.get("istemporary") or not name.endswith("_trace_unified") or not matches(name, match):
                    continue
                fqn = ".".join((cat, sch, name))
                item = {"kind": "unified_trace_table", "catalog": cat, "schema": sch,
                        "name": name, "qualified_name": fqn, "validation": "candidate",
                        "export_args": ["--table", fqn]}
                result["items"].append(item)
                try:
                    for part in (cat, sch, name):
                        quote(part)
                except ValueError:
                    item["export_args"] = None
                    issue(result, fqn, "identifier_not_supported_by_exporter")
                if not verify:
                    continue
                try:
                    columns = {r["col_name"].lower() for r in rows(cursor, "DESCRIBE TABLE " + scope + "." + quoted(name, '`'))}
                    missing = sorted({"trace_id", "spans", "request_time"} - columns)
                    item["validation"] = "missing_columns" if missing else "columns_present"
                    if missing:
                        item["missing_columns"] = missing
                        item["export_args"] = None
                except Exception as exc:
                    item["validation"] = "unverified"
                    issue(result, fqn, "column_check_failed", exc)
    result["items"].sort(key=lambda item: item["qualified_name"])
    return result


def clickhouse_discover(client, database=None, match="*"):
    from .clickhouse_source import DEFAULT_TABLE, identifier, inspect_schema, redact
    result = report("clickhouse")
    # system.tables lists only tables this user holds some grant on; no rows are read.
    sql = "SELECT database, name FROM system.tables WHERE name = {table:String} AND NOT is_temporary"
    parameters = {"table": DEFAULT_TABLE}
    if database:
        sql += " AND database = {database:String}"
        parameters["database"] = database
    for db, name in client.query(sql + " ORDER BY database", parameters=parameters).result_rows:
        if not matches(db, match):
            continue
        fqn = db + "." + name
        item = {"kind": "langfuse_events_table", "database": db, "name": name, "qualified_name": fqn,
                "validation": "candidate", "export_args": ["--database", db, "--table", name]}
        result["items"].append(item)
        try:
            identifier(db)
        except ValueError:
            item["export_args"] = None
            issue(result, fqn, "identifier_not_supported_by_exporter")
            continue
        try:
            schema = inspect_schema(client, db, name)
        except ValueError as exc:
            # Built from table metadata only; the URL redaction is a safeguard.
            item.update(validation="unsupported_schema", schema_problem=redact(exc), export_args=None)
            continue
        except Exception as exc:
            item["validation"] = "unverified"
            issue(result, fqn, "schema_check_failed", exc)
            continue
        item.update(validation="schema_supported", missing_optional_columns=schema["missing_optional_columns"])
    result["items"].sort(key=lambda item: item["qualified_name"])
    return result


def main(source, argv=None):
    cli = argparse.ArgumentParser(description="Discover visible agents or trace tables using metadata only.")
    if source == "clickhouse":
        cli.add_argument("--env-file", type=Path, help="Development .env file (default: clickhouse/.env when present)")
        cli.add_argument("--url", help="Override CLICKHOUSE_URL (http:// or https://, no credentials or path)")
        cli.add_argument("--database", help="Search only this database (default: every visible database; "
                                            "CLICKHOUSE_DB only selects the session database)")
        cli.add_argument("--match", default="*",
                         help="Case-insensitive database-name glob (* and ?), default: all; the table is always events_full")
    else:
        cli.add_argument("--connection", help="Named connector profile")
        cli.add_argument("--database" if source == "snowflake" else "--catalog")
        cli.add_argument("--schema")
        cli.add_argument("--match", default="*", help="Case-insensitive name glob (* and ?), default: all")
    output = cli.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true", help="Machine-readable result")
    output.add_argument("--text", action="store_true", help="Human-readable names and issues (default)")
    if source == "databricks":
        cli.add_argument("--env-file", type=Path)
        cli.add_argument("--http-path")
        cli.add_argument("--no-verify", action="store_true", help="Skip column checks; report name matches as candidates")
    args = cli.parse_args(argv)
    if source != "clickhouse" and args.schema and not (args.database if source == "snowflake" else args.catalog):
        cli.error("--schema requires " + ("--database" if source == "snowflake" else "--catalog"))
    if source == "databricks" and args.connection and args.env_file:
        cli.error("--connection cannot be combined with --env-file")
    try:
        if source == "clickhouse":
            result = run_clickhouse(args)
        elif source == "snowflake":
            import snowflake.connector
            options = {"connection_name": args.connection} if args.connection else {}
            with snowflake.connector.connect(**options) as connection, connection.cursor() as cursor:
                result = snowflake_discover(cursor, args.database, args.schema, args.match)
        else:
            from .databricks_source import DEFAULT_ENV, connect, settings
            values = settings(args.env_file or DEFAULT_ENV, connection=args.connection, http_path=args.http_path)
            with connect(values) as connection, connection.cursor() as cursor:
                result = databricks_discover(cursor, args.catalog, args.schema, args.match, not args.no_verify)
    except Exception as exc:
        result = report(source)
        issue(result, "discovery", "dependency_missing" if isinstance(exc, ImportError) else "discovery_failed", exc)
        result["status"] = "error"
    if not result["items"]:
        result["empty_result_note"] = (
            "No matching objects were returned; discovery failed or was incomplete."
            if result["status"] != "ok" else
            "No matching objects are visible in the requested scope. They may not exist, "
            "may not match the filters, or may be hidden by permissions.")
        if source == "snowflake":
            result["permission_guidance"] = [
                "Check the active role, requested scope/filters, and parent database/schema access. "
                "SHOW AGENTS visibility does not establish trace-read access.",
                "Reading agent traces requires SNOWFLAKE.CORTEX_USER and MONITOR (or OWNERSHIP) "
                "on the agent, plus access to the query warehouse.",
                "READ UNREDACTED AI OBSERVABILITY EVENTS TABLE ON ACCOUNT is an additional "
                "privilege for full payload content; its absence does not itself hide agents "
                "from discovery. See snowflake/README.md#permissions-and-unredacted-traces "
                "for administrator grant examples.",
            ]
        elif source == "clickhouse":
            result["permission_guidance"] = [
                "system.tables lists only tables on which this user holds a grant; check the requested "
                "--database/--match filters and the user's grants.",
                "Exporting requires SELECT on <database>.events_full; table visibility does not establish read access.",
            ]
    if not args.json:
        labels = {"metadata_only": "agent visible; trace access not checked",
                  "candidate": "name match; columns not checked",
                  "columns_present": "required columns present",
                  "missing_columns": "missing required columns",
                  "unverified": "could not check columns",
                  "schema_supported": "Langfuse v4 schema supported; read access not checked",
                  "unsupported_schema": "not a supported Langfuse v4 events table"}
        explanations = {
            "scope_query_failed": "Could not inspect this scope; it may be inaccessible.",
            "column_check_failed": "Could not inspect this table's columns; it may be inaccessible.",
            "schema_check_failed": "Could not inspect this table's engine and columns; it may be inaccessible.",
            "settings_invalid": "Invalid connection settings; check CLICKHOUSE_URL, CLICKHOUSE_USER, --env-file and --url.",
            "metadata_limit_reached_narrow_scope": "Metadata limit reached; narrow the database/schema scope.",
            "identifier_not_supported_by_exporter": "This object's name is not supported by the exporter.",
            "discovery_failed": "Discovery failed; check the connection profile and access permissions.",
            "dependency_missing": f"Install dependencies: python -m pip install -r {source}/requirements.txt",
        }
        for item in result["items"]:
            print(item["qualified_name"] + " [" + labels[item["validation"]] + "]")
            if item.get("schema_problem"):
                print("  " + item["schema_problem"])
        for entry in result["issues"]:
            print(entry["scope"] + ": " + explanations[entry["code"]], file=sys.stderr)
        state = {"ok": "complete", "partial": "incomplete", "error": "failed"}[result['status']]
        print(f"Found {len(result['items'])} matching objects; search {state}.")
        print(result.get("empty_result_note", result["visibility_note"]))
        for hint in result.get("permission_guidance", []):
            print(hint)
    else:
        print(json.dumps(result, ensure_ascii=False))
    return {"ok": 0, "error": 1, "partial": 3}[result["status"]]


def run_clickhouse(args):
    from contextlib import suppress
    from .clickhouse_source import connect, settings
    try:
        # The session uses the configured CLICKHOUSE_DB; --database only narrows the search,
        # so an invisible database yields an empty search rather than a connection error.
        values = settings(args.env_file, url=args.url)
    except ValueError as exc:
        result = report("clickhouse")
        issue(result, "discovery", "settings_invalid", exc)
        result["status"] = "error"
        return result
    client = connect(values)
    try:
        return clickhouse_discover(client, args.database, args.match)
    finally:
        with suppress(Exception):
            client.close()
