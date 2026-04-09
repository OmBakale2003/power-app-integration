from db.database import Database
from db.models import User
from collections import defaultdict
from config import DATABASE_URL

db_instance = Database(DATABASE_URL)

with db_instance.get_session() as db:
    data = db.query(User.office_location).all()
    job_titles = {row._mapping.get("office_location") for row in data}
    # converting to set


data_set = list(job_titles)


print("job_titles_len --> ", len(job_titles))
print("job_titles -->", data_set)


def normalize(s: str):
    if s is None:
        return None
    s = s.strip().lower()
    s = s.replace(" ", "")
    return s


def split_value(s: str):
    if "-" in s:
        return s.split("-", 1)
    return (s, None)


def group_data(data):
    groups = defaultdict(list)

    SPECIAL = {"system", "external", "disabled", "none", "commonaccount"}

    seen = set()  # to remove duplicates after normalization

    for item in data:
        norm = normalize(item)

        if norm in seen:
            continue
        seen.add(norm)

        if not norm:
            groups["invalid"].append(item)
            continue

        key, sub = split_value(norm)

        # classify special cases
        if key in SPECIAL:
            groups["system"].append(item)
        else:
            groups[key].append(item)

    return dict(groups)


result = group_data(data_set)

print("_____________________________________________________________________________")
print("grouping by segments")

max_len = max(len(k) for k in result.keys())

for key, val_list in result.items():
    print(key, " :")
    for val in val_list:
        print(" " * (max_len + 1), val)


def extract_location(s: str):
    if not s or "-" not in s:
        return None

    _, loc = s.split("-", 1)

    # ignore non-location keywords
    NON_LOCATION = {
        "techio",
        "finance",
        "commonaccount",
        "internalsystem",
        "techtransprojects",
        "p&c",
    }

    if loc in NON_LOCATION:
        return None

    return loc


def group_by_location(data):
    groups = defaultdict(list)
    seen = set()

    for item in data:
        norm = normalize(item)

        if norm in seen:
            continue
        seen.add(norm)

        loc = extract_location(norm)

        if loc:
            groups[loc].append(item)
        else:
            groups["unknown"].append(item)

    return dict(groups)


print(
    "______________________________________________________________________________________"
)
print("grouping by sub_location alone")

results_by_location = group_by_location(data_set)

max_len = max(len(k) for k in results_by_location.keys())

for key, val_list in results_by_location.items():
    print(key, " :")
    for val in val_list:
        print(" " * (max_len + 1), val)
