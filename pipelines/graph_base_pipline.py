import logging
from sqlalchemy.orm import Session
from pipelines.base_pipeline import BasePipeline
from data_extraction.graph_data_extractor import (
    GraphDataExtractor,
    DeltaTokenExpiredError,
)
from utils import dump_to_json

logger = logging.getLogger(__name__)


class GraphBasePipeline(BasePipeline):
    """
    Extends BasePipeline with Graph API specific behaviour:
      - Calls extractor with ENDPOINT and ENDPOINT_KEY
      - Holds pending delta link between extract() and run()
      - Saves delta link only after successful load

    Subclasses must define:
      - ENDPOINT     : Graph API path e.g. "users/delta"
      - ENDPOINT_KEY : stable key for delta link storage e.g. "users"
    And implement:
      - transform()
      - load()
    """

    ENDPOINT: str
    ENDPOINT_KEY: str

    def __init__(self, extractor: GraphDataExtractor):
        self.extractor = extractor
        self._pending_delta_link: str | None = None

    # Extract — shared by all Graph pipelines

    def extract(self) -> list[dict]:
        """
        Calls the extractor and holds the delta link internally.
        """
        data, self._pending_delta_link = self.extractor.extract(
            append_url=self.ENDPOINT, endpoint_key=self.ENDPOINT_KEY
        )
        return data

    # Run — overrides BasePipeline.run() with delta link + retry logic

    def run(self, session: Session) -> None:
        logger.info(f"[{self.__class__.__name__}] Starting pipeline run")

        try:
            self._execute(session)

        except DeltaTokenExpiredError:
            # Delta token stale — extractor already cleared it
            # Re-run will automatically fall back to full sync
            logger.warning(
                f"[{self.__class__.__name__}] "
                f"Delta token expired — retrying as full sync"
            )
            self._execute(session)

        logger.info(f"[{self.__class__.__name__}] Pipeline complete")

    # Internal

    def _execute(self, session: Session) -> None:
        """Single extract → transform → load cycle."""
        self._pending_delta_link = None

        raw = self.extract()
        # dump_to_json.dump(data=raw, endpoint_key=self.ENDPOINT_KEY)
        transformed = self.transform(raw)
        self.load(transformed, session)

        # Only reached if load() succeeded — safe to save delta link now
        if self._pending_delta_link:
            self.extractor.save_delta_link(self.ENDPOINT_KEY, self._pending_delta_link)
        else:
            logger.warning(
                f"[{self.__class__.__name__}] "
                f"No delta link received — possible page_limit hit or "
                f"endpoint does not support delta"
            )
