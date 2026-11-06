"""Stable command summaries; trace data remains in the output file."""
import json
import sys


def export_summary(source, args, result, downloaded_rows=None):
    capped = downloaded_rows is not None and args.limit is not None and downloaded_rows >= args.limit
    if capped:
        print(f"Row limit ({args.limit}) reached; results possibly truncated. No extra count query was run.", file=sys.stderr)
    if args.json_summary:
        print(json.dumps({"version": "1.0.0", "source": source,
                          "status": "ok" if result == 0 else "error",
                          "output": str(args.output) if args.output else "stdout", "format": args.format,
                          "output_written": result == 0,
                          "downloaded_rows": downloaded_rows,
                          "sample": args.sample, "limit": args.limit,
                          "since": args.since.isoformat() if args.since else None,
                          "until": args.until.isoformat() if args.until else None,
                          "query_timeout_seconds": args.query_timeout,
                          "warnings": getattr(args, "export_warnings", []),
                          "possibly_truncated": capped}))
    return result
