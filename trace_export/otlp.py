"""Dependency-free OTLP JSON encoding (no SDK-generated IDs or timestamps)."""

from contextlib import contextmanager
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import re
import tempfile


def identifier(value, length, label):
    # Span IDs may carry a suffix to distinguish reused Cortex planning IDs.
    pattern = f"[0-9a-fA-F]{{{length}}}" + ("" if length == 16 else "$")
    if not isinstance(value, str) or not re.match(pattern, value) or int(value[:length], 16) == 0:
        raise ValueError(f"invalid {label}: expected {length} nonzero hexadecimal digits" +
                         (" at the start" if length == 16 else ""))
    return value[:length].lower() + value[length:]


def unix_nano(value):
    # Snowflake NTZ timestamps are UTC; retain all nine fractional digits.
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    match = re.fullmatch(r"(\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?\s*(Z|[+-]\d\d:?\d\d)?", value)
    if not match:
        raise ValueError("unsupported timestamp format")
    base, fraction, offset = match.groups()
    dt = datetime.fromisoformat(base + (offset or "+00:00").replace("Z", "+00:00"))
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    result = (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int((fraction or "").ljust(9, "0"))
    if not 0 <= result < 2**64:
        raise ValueError("timestamp outside OTLP uint64 range")
    return str(result)


def any_value(value):
    if value is None:
        return {}
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ValueError("attribute integer outside OTLP int64 range")
        return {"intValue": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite attribute number")
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, list):
        return {"arrayValue": {"values": [any_value(v) for v in value]}}
    if isinstance(value, dict):
        return {"kvlistValue": {"values": attributes(value)}}
    raise ValueError("unsupported attribute value")


def attributes(values):
    return [{"key": key, "value": any_value(value)} for key, value in values.items()]


def envelope(span, resource, scope):
    return {"resourceSpans": [{"resource": {"attributes": attributes(resource)},
                               "scopeSpans": [{"scope": scope, "spans": [span]}]}]}


@contextmanager
def atomic_output(path, overwrite=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise FileExistsError("output exists; choose another path or use --overwrite")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".partial", delete=False) as handle:
            temporary = Path(handle.name)
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
