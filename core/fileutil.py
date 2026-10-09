"""Atomic file helpers — prevent corruption if the process crashes mid-write.

A plain `open(path, "w") + json.dump` leaves a truncated/corrupt file if the
process dies (or the box loses power) during the write. For config.json that
means a fatal start failure; for state files it breaks recovery. Writing to a
temp file in the same directory and then os.replace() makes the swap atomic on
POSIX, so readers always see either the old or the new complete file.
"""
import json
import os
import tempfile


def atomic_write_json(path, data, indent=2, ensure_ascii=False):
    """Serialize `data` to `path` atomically (temp file + os.replace + fsync)."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic on the same filesystem
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
