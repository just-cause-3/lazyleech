"""Dependency-free helpers for splitting large Telegram uploads."""

import os


TELEGRAM_SPLIT_SIZE = 2097152000
SPLIT_COPY_BUFFER_SIZE = 8 * 1024 * 1024


def _utf8_safe_tail(value, max_bytes):
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[-max_bytes:].decode("utf-8", errors="ignore")


def split_binary_file(filename, destination_dir, part_size=TELEGRAM_SPLIT_SIZE):
    """Split *filename* into deterministic, Telegram-sized numbered parts.

    GNU ``split --verbose`` writes its part list to stderr, and parsing that
    human-readable output is also locale-dependent. Build the parts directly
    so callers always receive the complete list that was actually created.
    """
    if part_size <= 0:
        raise ValueError("part_size must be greater than zero")

    os.makedirs(destination_dir, exist_ok=True)
    source_size = os.path.getsize(filename)
    # Leave five bytes for the dot and four-digit numeric suffix while keeping
    # multibyte Unicode characters intact.
    part_prefix = _utf8_safe_tail(os.path.basename(filename), 250)
    parts = []
    bytes_written = 0

    with open(filename, "rb") as source:
        part_number = 1
        while bytes_written < source_size:
            part_path = os.path.join(
                destination_dir, f"{part_prefix}.{part_number:04d}"
            )
            remaining = min(part_size, source_size - bytes_written)
            with open(part_path, "xb") as destination:
                while remaining:
                    chunk = source.read(min(SPLIT_COPY_BUFFER_SIZE, remaining))
                    if not chunk:
                        raise OSError(
                            "Source file ended before all split parts were written"
                        )
                    destination.write(chunk)
                    chunk_size = len(chunk)
                    remaining -= chunk_size
                    bytes_written += chunk_size
            parts.append(part_path)
            part_number += 1

    if not parts or sum(os.path.getsize(part) for part in parts) != source_size:
        raise OSError("Split parts do not match the source file size")
    return parts
