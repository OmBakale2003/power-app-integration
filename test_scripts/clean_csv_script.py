import pandas as pd
import ast
import os
 
print("CWD:", os.getcwd())
print("Files here:", os.listdir())
 
# ---- CONFIG ----
INPUT_EXCEL = r"test_scripts\graph_test_output\registered_devices_to_users_20260302_091536.csv"
OUTPUT_CSV = "output_normalized.csv"  # Changed to .csv
REGISTERED_USER_COL = "registeredUsers"
# ----------------
 
def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
 
 
df = pd.read_csv(INPUT_EXCEL)
 
expanded_rows = []
 
for _, row in df.iterrows():
    base_row = row.drop(labels=[REGISTERED_USER_COL]).to_dict()
    registered_user_raw = row[REGISTERED_USER_COL]
 
    if pd.isna(registered_user_raw):
        expanded_rows.append(base_row)
        continue
 
    try:
        registered_user_dict = ast.literal_eval(registered_user_raw)
    except Exception:
        expanded_rows.append(base_row)
        continue
 
    flat_user = flatten_dict(registered_user_dict)
    flat_user_prefixed = {f"registered_user_{k}": v for k, v in flat_user.items()}
    combined = {**base_row, **flat_user_prefixed}
    expanded_rows.append(combined)
 
final_df = pd.DataFrame(expanded_rows)
 
# Export as CSV
final_df.to_csv(OUTPUT_CSV, index=False)  # Changed from to_excel() to to_csv()
 
print("✅ CSV normalized successfully!")