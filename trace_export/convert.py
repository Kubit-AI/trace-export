#!/usr/bin/env python3
"""Convert raw Cortex JSONL into GenAI OTLP JSONL without database access."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys

from trace_export.cortex import convert_row
from trace_export.otlp import atomic_output


def main(argv=None):
    parser = argparse.ArgumentParser(description="Convert raw trace JSONL into GenAI OTLP JSONL locally.")
    parser.add_argument("--source", choices=("snowflake", "databricks", "clickhouse", "logfire"), default="snowflake")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--skip-invalid", action="store_true", help="Skip malformed rows with line-number diagnostics")
    parser.add_argument("--input-token-mode", choices=("auto", "includes-cache", "excludes-cache"), default="auto",
                        help="Whether Snowflake input count includes cache tokens; auto omits ambiguous totals")
    args = parser.parse_args(argv)
    if args.input.resolve() == args.output.resolve() or (args.input.exists() and args.output.exists() and args.input.samefile(args.output)):
        parser.error("input and output must be different files")
    counts, warnings, notes = Counter(), Counter(), Counter()
    try:
        with args.input.open(encoding="utf-8") as source, atomic_output(args.output, args.overwrite) as dest:
            for line_number, line in enumerate(source, 1):
                counts["rows"] += 1
                try:
                    row = json.loads(line)
                    if args.source == "databricks":
                        from trace_export.mlflow import convert_row as convert_mlflow
                        converted, issues = convert_mlflow(row)
                    elif args.source == "clickhouse":
                        from trace_export.langfuse import convert_row as convert_langfuse
                        converted, issues = convert_langfuse(row)
                    elif args.source == "logfire":
                        from trace_export.logfire import convert_row as convert_logfire
                        converted, issues = convert_logfire(row)
                    else:
                        converted, issues = convert_row(row, args.input_token_mode)
                    payload = json.dumps(converted, ensure_ascii=False, allow_nan=False, separators=(",", ":")) if converted else None
                except (ValueError, TypeError, OverflowError) as exc:
                    # No raw payloads in diagnostics.
                    message = f"line {line_number}: {exc}"
                    if not args.skip_invalid:
                        raise ValueError(message) from exc
                    counts["invalid"] += 1
                    print(f"Skipped {message}", file=sys.stderr)
                    continue
                if converted is None:
                    counts["non_span"] += 1
                    continue
                for issue in set(issues):
                    if issue in ("output_finish_reason_unavailable", "span_timing_corrected"):
                        notes[issue] += 1
                        continue
                    warnings[issue] += 1
                    if warnings[issue] <= 3:
                        print(f"Warning line {line_number}: {issue}", file=sys.stderr)
                dest.write(payload + "\n")
                counts["spans"] += sum(len(scope["spans"]) for resource in converted["resourceSpans"] for scope in resource["scopeSpans"])
    except (OSError, ValueError) as exc:
        print(f"Conversion failed: {exc}", file=sys.stderr)
        return 1
    print(f"Converted {counts['spans']} spans from {counts['rows']} rows; "
          f"skipped {counts['non_span']} non-span and {counts['invalid']} invalid rows. Output: {args.output}", file=sys.stderr)
    if warnings:
        print("Mapping warnings (number of affected rows): " + json.dumps(dict(warnings), sort_keys=True), file=sys.stderr)
    if notes:
        print("Conversion notes (number of affected rows): " + json.dumps(dict(notes), sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
