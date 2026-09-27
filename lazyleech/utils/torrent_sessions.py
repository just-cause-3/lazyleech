"""Persistent parent/child sessions for workspace-split torrents."""

from .terabox_sessions import FILE_UPLOADED, TeraboxSessionStore


class TorrentSessionStore(TeraboxSessionStore):
    """Isolated torrent-chain collections using the shared chain state machine."""

    def __init__(self, db_url=None, database_name=None):
        super().__init__(
            db_url=db_url,
            database_name=database_name,
            collection_prefix="TORRENT",
        )


torrent_session_store = TorrentSessionStore()


__all__ = ["FILE_UPLOADED", "TorrentSessionStore", "torrent_session_store"]
