import os
import logging
import time
from requests.exceptions import SSLError

from db.database import Database
from data_extraction.graph_data_extractor import GraphDataExtractor
from pipelines.user_data_pipeline import UsersPipeline
from pipelines.device_data_pipeline import DevicesPipeline
from pipelines.managed_device_data_pipeline import ManagedDevicesPipeline

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

logger = logging.getLogger(__name__)

# Note: In production, ensure DATABASE_URL is an Azure PostgreSQL connection string
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data.db")
db = Database(DATABASE_URL)
extractor = GraphDataExtractor()

MAX_RETRIES = 3


def run_pipelines():
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            with db.get_session() as session:
                UsersPipeline(extractor).run(session)

            devices_pipeline = DevicesPipeline(extractor)
            with db.get_session() as session:
                devices_pipeline.run(session)

            with db.get_session() as session:
                devices_pipeline.backfill_user_ids(session)

            with db.get_session() as session:
                ManagedDevicesPipeline(extractor).run(session)

            logger.info("All pipelines completed successfully")
            return

        except SSLError as e:
            attempt += 1
            wait = 2**attempt
            logger.warning(
                f"SSLError attempt {attempt}/{MAX_RETRIES}: {e} - retrying in {wait}s"
            )
            if attempt >= MAX_RETRIES:
                logger.error("Max retries reached - giving up on this run")
                return
            time.sleep(wait)

        except Exception as e:
            logger.error(
                f"Pipeline run failed with non-retryable error: {e}", exc_info=True
            )
            return


def setup_scheduler() -> BackgroundScheduler:
    """Configures and returns the background scheduler."""
    # Ensure tables are created first
    db.create_tables()

    jobstores = {"default": SQLAlchemyJobStore(url=DATABASE_URL)}

    scheduler = BackgroundScheduler(jobstores=jobstores, timezone="UTC")

    # Add the daily job
    scheduler.add_job(
        func=run_pipelines,
        trigger=CronTrigger(hour=1, minute=0),
        id="graph_sync",
        max_instances=1,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # Optional: Initial run on startup
    scheduler.add_job(func=run_pipelines, id="immediate_run", replace_existing=True)

    return scheduler
