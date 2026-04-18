"""Crawl4AI service entry point."""
import asyncio
import logging
import sys

sys.path.insert(0, "/app")

from crawl4ai.crawl4ai_service import Crawl4AIService
from shared.metrics import start_metrics_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)


async def main():
    start_metrics_server(9092)
    service = Crawl4AIService()
    try:
        await service.run_loop()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await service.close()


if __name__ == "__main__":
    asyncio.run(main())
