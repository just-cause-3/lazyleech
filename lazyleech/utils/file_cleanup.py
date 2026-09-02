"""Safe cleanup helpers for completed upload source files."""

import os


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
