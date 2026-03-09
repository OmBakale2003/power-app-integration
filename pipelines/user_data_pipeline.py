import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from db.models import User
from pipelines.graph_base_pipline import GraphBasePipeline
from data_extraction.graph_data_extractor import GraphDataExtractor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# Sentinel
# Distinguishes "field absent in delta response" from "field explicitly nulled"
# _MISSING  → Graph didn't mention this field — don't touch the DB column
# None      → Graph explicitly sent null    — clear the DB column
_MISSING = object()


# Field registry
# Fields with dedicated columns — stripped from raw_json storage.
MAPPED_FIELDS = {
    "id",
    "displayName",
    "givenName",
    "surname",
    "mail",
    "userPrincipalName",
    "jobTitle",
    "officeLocation",
    "businessPhones",
    "mobilePhone",
    "preferredLanguage",
    "manager@delta",
    "@removed",
    # odata noise — present on every record, not useful to store in raw_json
    "@odata.type",
    "@odata.id",
    "@odata.context",
}

# (db_column_name, graph_api_field_name) for all simple scalar fields
FIELD_MAP: list[tuple[str, str]] = [
    ("display_name", "displayName"),
    ("given_name", "givenName"),
    ("surname", "surname"),
    ("mail", "mail"),
    ("user_principal_name", "userPrincipalName"),
    ("job_title", "jobTitle"),
    ("office_location", "officeLocation"),
    ("mobile_phone", "mobilePhone"),
    ("preferred_language", "preferredLanguage"),
]


# Pipeline


class UsersPipeline(GraphBasePipeline):
    ENDPOINT = "users/delta?$expand=registeredDevices"
    ENDPOINT_KEY = "users"

    def __init__(self, extractor: GraphDataExtractor):
        super().__init__(extractor)

    # Transform

    def transform(self, data: list[dict]) -> list[dict]:
        transformed = []

        for record in data:
            is_deleted = "@removed" in record

            # businessPhones is a list — needs joining before storage
            phones_raw = record.get("businessPhones", _MISSING)
            if phones_raw is _MISSING:
                business_phones = _MISSING  # field absent — don't touch column
            elif phones_raw:
                business_phones = ", ".join(phones_raw)
            else:
                business_phones = None  # [] or null — clear the column

            entry: dict = {
                "id": record.get("id"),
                "is_deleted": is_deleted,
                "deleted_at": datetime.now(timezone.utc) if is_deleted else _MISSING,
                "business_phones": business_phones,
                "raw_json": _strip_meta(record),
            }

            # Simple scalar fields — all use _MISSING as sentinel default
            for col, graph_key in FIELD_MAP:
                entry[col] = record.get(graph_key, _MISSING)

            transformed.append(entry)

        logger.info(
            f"[{self.ENDPOINT_KEY}] Transformed {len(transformed)} records "
            f"({sum(1 for r in transformed if r['is_deleted'])} deletes)"
        )
        return transformed

    def load(self, data: list[dict], session: Session) -> None:
        now = datetime.now(timezone.utc)
        upserted = 0
        deleted = 0

        # Deduplicate + merge duplicate records
        # Graph delta can return the same ID multiple times across pages:
        #   - Exact duplicate     → same record sent twice (keep either)
        #   - Partial duplicate   → later record has only changed fields
        #   - Delete after seen   → later record is @removed, wins unconditionally
        deduped: dict[str, dict] = {}
        for record in data:
            uid = record["id"]
            if uid not in deduped:
                deduped[uid] = record
            else:
                if record["is_deleted"]:
                    # Deletion always wins regardless of order
                    deduped[uid] = record
                else:
                    # Merge: overlay non-_MISSING fields from later record
                    # onto earlier record so no data from either is lost
                    existing = deduped[uid]
                    for key, val in record.items():
                        if val is not _MISSING:
                            existing[key] = val

        if len(deduped) < len(data):
            logger.warning(
                f"[{self.ENDPOINT_KEY}] Deduplicated {len(data) - len(deduped)} "
                f"duplicate record(s) before load"
            )

        # Upsert loop
        for record in deduped.values():
            user = session.get(User, record["id"])

            if user is None:
                user = User(id=record["id"])
                session.add(user)
                is_new = True
            else:
                is_new = False

            # Soft delete
            if record["is_deleted"]:
                user.is_deleted = True
                user.deleted_at = record["deleted_at"]
                user.synced_at = now
                deleted += 1
                continue

            # Scalar fields
            for col, _ in FIELD_MAP:
                val = record[col]
                if val is not _MISSING:
                    setattr(user, col, val)

            # business_phones
            if record["business_phones"] is not _MISSING:
                user.business_phones = record["business_phones"]

            # raw_json
            if record["raw_json"] is not None:
                existing = user.raw_json or {}
                user.raw_json = {**existing, **record["raw_json"]}
            elif is_new:
                user.raw_json = None

            user.is_deleted = False
            user.deleted_at = None
            user.synced_at = now
            upserted += 1

        # Database.get_session() context manager owns the transaction
        logger.info(
            f"[{self.ENDPOINT_KEY}] Load complete — "
            f"{upserted} upserted, {deleted} soft-deleted"
        )


# Helpers


def _strip_meta(record: dict) -> dict | None:
    """
    Returns only unmapped fields for raw_json storage.
    Mapped fields already have dedicated columns — no need to double-store them.
    Returns None if no unmapped extras exist (avoids storing empty {}).
    """
    extra = {k: v for k, v in record.items() if k not in MAPPED_FIELDS}
    return extra if extra else None
