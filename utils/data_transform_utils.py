import unicodedata

# Normalization


def normalize(s: str) -> str | None:
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.strip().lower().replace(" ", "")
    return s or None


# City / Location Mapping

CITY_MAP = {
    # India
    "bangalore": "bengaluru",
    "bengaluru": "bengaluru",
    "pune": "pune",
    "chennai": "chennai",
    "hyderabad": "hyderabad",
    "mumbai": "mumbai",
    "delhi": "delhi",
    "noida": "noida",
    "ahmedabad": "ahmedabad",
    "gurgaon": "gurugram",
    "gurugram": "gurugram",
    # USA
    "newjersey": "new jersey",
    "nj": "new jersey",
    "santaclara": "santa clara",
    "mountainview": "mountain view",
    "millcreek": "mill creek",
    "sanjose": "san jose",
    "sanramon": "san ramon",
    "scottsdale": "scottsdale",
    "seattle": "seattle",
    "austin": "austin",
    "dallas": "dallas",
    "phoenix": "phoenix",
    "pittsburgh": "pittsburgh",
    "marietta": "marietta",
    "indianapolis": "indianapolis",
    "rochellepark": "rochelle park",
    "woodlandpark": "woodland park",
    "orinda": "orinda",
    "wayne": "wayne",
    "california": "california",
    # Europe
    "london": "london",
    "lon": "london",
    "barcelona": "barcelona",
    "bucharest": "bucharest",
    "craiova": "craiova",
    "skopje": "skopje",
    "spain": "spain",
    # Latin America
    "lima": "lima",
    "argentina": "argentina",
    "heredia": "heredia",
    "cali": "cali",
    "panama": "panama",
    "campinas": "campinas",
    "chihuahua": "chihuahua",
    "hermosillo": "hermosillo",
    "merida": "merida",
    "sanluispotosi": "san luis potosi",
    "mexicocity": "mexico city",
    "cochabamba": "cochabamba",
    # Asia Pacific
    "singapore": "singapore",
    "malaysia": "malaysia",
    "kualalumpur": "kuala lumpur",
    "manila": "manila",
    "melbourne": "melbourne",
    "sydney": "sydney",
}


# Non-Location Suffixes

NON_LOCATION = {
    "techio",
    "finance",
    "commonaccount",
    "internalsystem",
    "techtransprojects",
    "p&c",
    "tech",
    "hq",
    "coe",
    "remote",
    "us",
    "usa",
    "canada",
}


# Parse a single office_location string


def parse_item(item: str) -> tuple[str, str]:
    norm = normalize(item)

    if not norm:
        return ("unknown", "unknown")

    if "-" not in norm:
        return (norm, "unknown")

    region, suffix = norm.split("-", 1)

    region = region or "unknown"

    # Strip trailing state/country code e.g. "seattle,wa"
    if "," in suffix:
        suffix = suffix.split(",")[0]

    if not suffix or suffix in NON_LOCATION:
        return (region, "non_location")

    location = CITY_MAP.get(suffix, "unknown")

    return (region, location)


# Main Grouping Function


def group_office_location_to_flat_table(data: list[str | None]) -> list[dict]:
    rows = []

    for item in data:
        if not item or not item.strip():
            continue

        region, location = parse_item(item)

        rows.append(
            {
                "region": region,
                "location": location,
                "original_value": item,
            }
        )

    return rows
