from graph import fetch_users
from utils.csv_utils import json_to_csv
from data_extraction.graph_data_extractor import GraphDataExtractor

def main():
    dataExtractor = GraphDataExtractor()
    dataExtractor.extract(append_url="/users/delta?$top=999")
    print("the delta link to fetch next changes is ---> ",dataExtractor.delta_link)

if __name__ == "__main__":
    main()