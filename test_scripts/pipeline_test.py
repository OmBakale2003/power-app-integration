import logging
from db.database import Database
from data_extraction.graph_data_extractor import GraphDataExtractor
from pipelines.user_data_pipeline import UsersPipeline
from pipelines.device_data_pipeline import DevicesPipeline
from pipelines.managed_device_data_pipeline import ManagedDevicesPipeline

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    db = Database("sqlite:///data.db")
    db.create_tables()

    extractor = GraphDataExtractor()

    # Sync users
    """ with db.get_session() as session:
        UsersPipeline(extractor).run(session)

    # Sync devices
    devices_pipeline = DevicesPipeline(extractor)
    with db.get_session() as session:
        devices_pipeline.run(session)

    # Backfill device → user FK
    # Runs after BOTH pipelines are committed so all FKs are resolvable
    with db.get_session() as session:
        devices_pipeline.backfill_user_ids(session) """

    # Sync managedDevices
    with db.get_session() as session:
        ManagedDevicesPipeline(extractor).run(session)


if __name__ == "__main__":
    main()
