"""
Initialize Qdrant collections for Frontier Intelligence.
Run: python storage/qdrant/collections.py
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.init_storage import init_qdrant
from shared.config import get_settings


async def init_collections(qdrant_url: str | None = None) -> None:
    settings = get_settings()
    if qdrant_url:
        settings.qdrant_url = qdrant_url
    await init_qdrant(settings)


if __name__ == "__main__":
    asyncio.run(init_collections(os.getenv("QDRANT_URL")))
