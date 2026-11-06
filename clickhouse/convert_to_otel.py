#!/usr/bin/env python3
"""Convert a saved ClickHouse Langfuse events export into OTLP JSONL."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trace_export.convert import main

if __name__ == "__main__":
    raise SystemExit(main([*sys.argv[1:], "--source", "clickhouse"]))
