from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class BasePipeline(ABC):
    @abstractmethod
    def extract(
        self,
    ) -> list[dict]:
        """Pulls data from the graph"""

    @abstractmethod
    def transform(self, data: list[dict]) -> list[dict]:
        """Clean/reshape data"""

    @abstractmethod
    def load(self, data: list[dict], session: Session) -> None:
        """load to db"""

    @abstractmethod
    def run(self, session: Session) -> None:
        """runs the data pipline orchestration"""
