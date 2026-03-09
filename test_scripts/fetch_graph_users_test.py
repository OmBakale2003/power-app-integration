from graph import fetch_users
from utils import dump_to_json
from utils.csv_utils import json_to_csv
from data_extraction.graph_data_extractor import GraphDataExtractor


def main():
    dataExtractor = GraphDataExtractor()
    data, delta_link = dataExtractor.extract(
        append_url="/users?$expand=registeredDevices($select=id)",
        endpoint_key="users_to_devices",
        page_limit=15,
    )

    dump_to_json.dump(data=data, endpoint_key="users_to_devices")


if __name__ == "__main__":
    main()
