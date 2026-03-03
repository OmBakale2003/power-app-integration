from dataverse import create_dataverse_row,upload_file_to_dataverse,download_csv_file
from datetime import datetime
import config
import auth
from pathlib import Path

def main():
    # 1️⃣ Data for the new row (logical names only)
    row_data = {
        "cr277_source_timestamp_part": str(datetime.utcnow().strftime("%Y%m%d_%H%M%S")),
    }

    # 2️⃣ Create the row
    table_logical_name = "cr277_source_csv"
    entity_set_name = "cr277_source_csvs"
    dataverse_access_token = auth.get_dataverse_token()

    row_id = create_dataverse_row(
        access_token= dataverse_access_token,
        environment_url=config.ENV_URL,
        entity_set_name=entity_set_name,
        table_logical_name=table_logical_name,
        row_data=row_data
    )

    print(f"Row created with ID: {row_id}")
    file_column = "cr277_csv_file"
    file_path = Path("test_scripts")/"graph_test_output"/"graph_users_20260208_183612.csv"
    print("trying to upload file -> "+ str(file_path))

    # 3️⃣ Upload the CSV file to the File column
    dataverse_access_token = auth.get_dataverse_token()
    # example -> https://orgf7e4d7bc.crm8.dynamics.com/api/data/v9.2/cr277_graph_to_dvs(15dfa306-3805-f111-9a20-7ced8d9ed123)
    entity_uri = f"{config.ENV_URL}/api/data/v9.2/{entity_set_name}({row_id})"
    print("uploading to entity uri ->",entity_uri)

    result = upload_file_to_dataverse(access_token=dataverse_access_token,
                                      environment_url=config.ENV_URL,
                                      entity_logical_name=table_logical_name,
                                      entity_id=row_id,
                                      primary_key_name= "cr277_source_csvid",
                                      file_column_name= "cr277_csv_file",
                                      file_path=file_path,
                                      max_size_kb=50*1024*1024,
                                      )
    
    #download the same csv 
    output_path = str(Path("test_scripts")/"graph_test_output"/"graph_users.csv")
    download_csv_file(entity_uri=entity_uri,file_column=file_column,output_path=output_path)

    print(result)
    print("End-to-end test completed successfully")

if __name__ == "__main__":
    main()
