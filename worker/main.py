"""Worker service — asyncio supervisor for all task consumers."""
import asyncio
import logging
import sys

sys.path.insert(0, "/app")

from worker.tasks.enrichment_task import EnrichmentTask
from worker.tasks.reindex_task import ReindexTask
from worker.tasks.vision_task import VisionTask
from shared.metrics import start_metrics_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    start_metrics_server(9090)
    enrichment = EnrichmentTask()
    reindex = ReindexTask()
    vision = VisionTask()
    try:
        tasks = [enrichment.run_loop(), reindex.run_loop(), vision.run_loop()]
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await enrichment.close()
        await reindex.close()
        await vision.close()


if __name__ == "__main__":
    asyncio.run(main())
