import logging
import requests
import time
from auth import get_graph_token
from data_loader.simple_csv_loader import SimpleCSVLoader


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


class GraphDataExtractor:

    def __init__(self):
        self.delta_link = None
        self.base_url = "https://graph.microsoft.com/v1.0/"

    def _build_url(self, append_url: str) -> str:
        if append_url.startswith("https://"):
            return append_url
        return self.base_url + append_url.lstrip("/")

    def extract(self, append_url: str, file_name: str | None = None, write_to_csv : bool = False, page_limit:int | None = None):

        logger.info("Starting Graph extraction")

        token = get_graph_token()

        if self.delta_link:
            url = self.delta_link
            logger.info("Using stored delta link")
        else:
            url = self._build_url(append_url)
            logger.info(f"Initial URL: {url}")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

        all_results = []
        total_records = 0
        page_count = 0

        while url:
            logger.info(f"Fetching page {page_count + 1}")
            
            if(page_count != None and page_count == page_limit):
                logger.info(f"Hit the page limit stopping from calling the next link")
                break

            try:
                resp = requests.get(url, headers=headers, timeout=30)

                if resp.status_code == 429:
                    retry = int(resp.headers.get("Retry-After", 5))
                    logger.warning(f"Throttled. Retrying after {retry} seconds...")
                    time.sleep(retry)
                    continue

                resp.raise_for_status()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                raise

            data = resp.json()
            batch = data.get("value", [])

            batch_size = len(batch)
            total_records += batch_size
            page_count += 1

            logger.info(f"Fetched {batch_size} records (Total: {total_records})")

            all_results.extend(batch)

            # Pagination
            next_link = data.get("@odata.nextLink")
            if next_link:
                logger.info("Next page detected")
            url = next_link

            # Delta token
            if "@odata.deltaLink" in data:
                self.delta_link = data["@odata.deltaLink"]
                logger.info("Delta link received and stored")

        logger.info(f"Extraction complete. Total records: {total_records}")

        if(write_to_csv):
            logger.info("Writing data to CSV")
            csv_loader = SimpleCSVLoader()
            assert file_name != None , "file_name of csv to be created should not be None"
            csv_loader.loadDataIntoCSV(filename= file_name, data=all_results)
            logger.info("CSV creation completed")

        return all_results