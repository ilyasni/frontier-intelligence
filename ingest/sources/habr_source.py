"""Backward-compatible alias for the Habr RSS preset."""
from __future__ import annotations

from ingest.sources.rss_source import RSSSource


class HabrSource(RSSSource):
    pass
