from utils.csv_utils import json_to_csv
from graph import get_req_custom_url
from app.data_extraction.graph_data_extractor import GraphDataExtractor

def main():  
    data_extractor = GraphDataExtractor();
    data_extractor.extract(append_url="devices?$top=999",
                           file_name="all_ad_devices",
                           page_limit=None,
                           write_to_csv=True)

if __name__ == "__main__":
    main()