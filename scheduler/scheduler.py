import logging
import time
import signal
import sys

from requests.exceptions import SSLError

from db.database import Database
from data_extraction.graph_data_extractor import GraphDataExtractor
from pipelines.user_data_pipeline import UsersPipeline
from pipelines.device_data_pipeline import DevicesPipeline
from pipelines.managed_device_data_pipeline import ManagedDevicesPipeline

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

db = Database("sqlite:///data.db")
extractor = GraphDataExtractor()

MAX_RETRIES = 3


def run():
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            # ── Users ──────────────────────────────────────────────────────
            with db.get_session() as session:
                UsersPipeline(extractor).run(session)

            # ── Devices ────────────────────────────────────────────────────
            devices_pipeline = DevicesPipeline(extractor)
            with db.get_session() as session:
                devices_pipeline.run(session)

            # ── Backfill device → user FK ──────────────────────────────────
            with db.get_session() as session:
                devices_pipeline.backfill_user_ids(session)

            # ── Managed Devices ────────────────────────────────────────────
            with db.get_session() as session:
                ManagedDevicesPipeline(extractor).run(session)

            # All pipelines succeeded — exit retry loop
            logger.info("All pipelines completed successfully")
            return

        except SSLError as e:
            attempt += 1
            wait = 2**attempt  # 2s, 4s, 8s
            logger.warning(
                f"SSLError on attempt {attempt}/{MAX_RETRIES}: {e} "
                f"— retrying in {wait}s"
            )
            if attempt >= MAX_RETRIES:
                logger.error("Max retries reached — giving up on this run")
                return
            time.sleep(wait)

        except Exception as e:
            # Non-SSL failures are not retried — log and exit
            logger.error(
                f"Pipeline run failed with non-retryable error: {e}", exc_info=True
            )
            return


if __name__ == "__main__":
    db.create_tables()

    scheduler = BlockingScheduler(timezone="UTC")

    scheduler.add_job(
        func=run,
        trigger=CronTrigger(hour=1, minute=0),
        id="graph_sync",
        max_instances=1,
        misfire_grace_time=3600,
    )

    signal.signal(
        signal.SIGINT, lambda s, f: (scheduler.shutdown(wait=False), sys.exit(0))
    )
    signal.signal(
        signal.SIGTERM, lambda s, f: (scheduler.shutdown(wait=False), sys.exit(0))
    )

    logger.info("Scheduler started — running immediately then daily at 01:00 UTC")
    run()

    try:
        scheduler.start()
    except Exception as e:
        logger.error(f"Scheduler crashed: {e}", exc_info=True)
        sys.exit(1)
