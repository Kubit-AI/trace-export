#!/usr/bin/env python3
"""Discover visible Snowflake Cortex agents."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from trace_export.discovery import main

if __name__ == "__main__":
    raise SystemExit(main("snowflake"))
