from db.database import Database
from db.importer import run_import

db = Database("sqlite:///graph.db")
db.create_tables()

with db.get_session() as session:
    run_import(
        users_csv="./test_scripts/graph_test_output/all_users_registered_devices_20260302_104806.csv",
        devices_csv="./test_scripts/graph_test_output/all_ad_devices_20260303_084608.csv",
        managed_devices_csv="./test_scripts/graph_test_output/all_managed_devices_20260302_085539.csv",
        session=session
    )