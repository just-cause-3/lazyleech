"""Safe cleanup helpers for completed and abandoned download files."""

import os
import re
import shutil


_USER_DIRECTORY_RE = re.compile(r"^\d+$")
_TIMESTAMP_JOB_DIRECTORY_RE = re.compile(r"^\d+(?:\.\d+)?$")
_SESSION_JOB_DIRECTORIES = frozenset({"bunkr_sessions", "terabox_sessions"})


def _is_abandoned_job_directory(name):
    return (
        name in _SESSION_JOB_DIRECTORIES
        or bool(_TIMESTAMP_JOB_DIRECTORY_RE.fullmatch(name))
        or name.startswith("tmp")
    )


def clear_abandoned_download_directories(*roots):
    """Remove recognized runtime job directories without touching user assets.

    A root is scanned only for numeric Telegram-user directories. Symlinks are
    never followed or removed, and unrelated files/directories are preserved.
    The returned paths are the directories that were successfully removed.
    """
    removed = []
    seen_roots = set()
    for root_value in roots:
        if not root_value:
            continue
        root = os.path.realpath(os.path.abspath(root_value))
        if root in seen_roots or not os.path.isdir(root):
            continue
        seen_roots.add(root)
        try:
            user_entries = list(os.scandir(root))
        except OSError:
            continue
        for user_entry in user_entries:
            if (
                not _USER_DIRECTORY_RE.fullmatch(user_entry.name)
                or user_entry.is_symlink()
                or not user_entry.is_dir(follow_symlinks=False)
            ):
                continue
            user_root = os.path.realpath(user_entry.path)
            try:
                if os.path.commonpath((root, user_root)) != root:
                    continue
                job_entries = list(os.scandir(user_root))
            except (OSError, ValueError):
                continue
            for job_entry in job_entries:
                if (
                    not _is_abandoned_job_directory(job_entry.name)
                    or job_entry.is_symlink()
                    or not job_entry.is_dir(follow_symlinks=False)
                ):
                    continue
                job_path = os.path.realpath(job_entry.path)
                try:
                    if os.path.commonpath((user_root, job_path)) != user_root:
                        continue
                    shutil.rmtree(job_path)
                except (OSError, ValueError):
                    continue
                removed.append(job_path)
            try:
                if not os.listdir(user_root):
                    os.rmdir(user_root)
            except OSError:
                pass
    return removed


def remove_uploaded_source(filepath, download_root):
    """Remove one uploaded source and prune its empty subdirectories.

    The download root itself is retained for the job-level cleanup callback.
    Paths resolving outside that root are refused.
    """
    if not filepath or not download_root:
        return False

    root = os.path.realpath(os.path.abspath(download_root))
    source = os.path.realpath(os.path.abspath(filepath))
    try:
        if os.path.commonpath((root, source)) != root:
            return False
    except ValueError:
        return False

    if not os.path.isfile(source):
        return False
    os.remove(source)

    parent = os.path.dirname(source)
    while parent != root:
        try:
            os.rmdir(parent)
        except OSError:
            break
        next_parent = os.path.dirname(parent)
        if next_parent == parent:
            break
        parent = next_parent
    return True
