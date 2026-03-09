import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from db.models import Device, User
from pipelines.graph_base_pipline import GraphBasePipeline
from data_extraction.graph_data_extractor import GraphDataExtractor
from data_extraction.device_to_user_mapping_extractor import build_user_device_mapping

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

_MISSING = object()

MAPPED_FIELDS = {
    "id",
    "accountEnabled",
    "alternativeSecurityIds",
    "approximateLastSignInDateTime",
    "complianceExpirationDateTime",
    "createdDateTime",
    "deletedDateTime",
    "deviceCategory",
    "deviceId",
    "deviceMetadata",
    "deviceOwnership",
    "deviceVersion",
    "displayName",
    "domainName",
    "enrollmentProfileName",
    "enrollmentType",
    "externalSourceName",
    "isCompliant",
    "isManaged",
    "isRooted",
    "managementType",
    "manufacturer",
    "mdmAppId",
    "model",
    "onPremisesLastSyncDateTime",
    "onPremisesSyncEnabled",
    "operatingSystem",
    "operatingSystemVersion",
    "physicalIds",
    "profileType",
    "registrationDateTime",
    "sourceType",
    "systemLabels",
    "trustType",
    "@removed",
    "@odata.type",
    "@odata.id",
    "@odata.context",
}

FIELD_MAP: list[tuple[str, str]] = [
    ("account_enabled", "accountEnabled"),
    ("alternative_security_ids", "alternativeSecurityIds"),
    ("approximate_last_signin_datetime", "approximateLastSignInDateTime"),
    ("compliance_expiration_datetime", "complianceExpirationDateTime"),
    ("created_datetime", "createdDateTime"),
    ("deleted_datetime", "deletedDateTime"),
    ("device_category", "deviceCategory"),
    ("device_id", "deviceId"),
    ("device_metadata", "deviceMetadata"),
    ("device_ownership", "deviceOwnership"),
    ("device_version", "deviceVersion"),
    ("display_name", "displayName"),
    ("domain_name", "domainName"),
    ("enrollment_profile_name", "enrollmentProfileName"),
    ("enrollment_type", "enrollmentType"),
    ("external_source_name", "externalSourceName"),
    ("is_compliant", "isCompliant"),
    ("is_managed", "isManaged"),
    ("is_rooted", "isRooted"),
    ("management_type", "managementType"),
    ("manufacturer", "manufacturer"),
    ("mdm_app_id", "mdmAppId"),
    ("model", "model"),
    ("on_premises_last_sync_datetime", "onPremisesLastSyncDateTime"),
    ("on_premises_sync_enabled", "onPremisesSyncEnabled"),
    ("operating_system", "operatingSystem"),
    ("operating_system_version", "operatingSystemVersion"),
    ("physical_ids", "physicalIds"),
    ("profile_type", "profileType"),
    ("registration_datetime", "registrationDateTime"),
    ("source_type", "sourceType"),
    ("system_labels", "systemLabels"),
    ("trust_type", "trustType"),
]

# DateTime fields that need ISO string → datetime conversion
DATETIME_COLS = {
    "approximate_last_signin_datetime",
    "compliance_expiration_datetime",
    "created_datetime",
    "deleted_datetime",
    "on_premises_last_sync_datetime",
    "registration_datetime",
}


class DevicesPipeline(GraphBasePipeline):
    ENDPOINT = (
        "devices/delta"
        "?$select=id,accountEnabled,alternativeSecurityIds,approximateLastSignInDateTime,"
        "complianceExpirationDateTime,createdDateTime,deletedDateTime,deviceCategory,"
        "deviceId,deviceMetadata,deviceOwnership,deviceVersion,displayName,domainName,"
        "enrollmentProfileName,enrollmentType,extensionAttributes,externalSourceName,"
        "isCompliant,isManaged,isRooted,managementType,manufacturer,mdmAppId,model,"
        "onPremisesLastSyncDateTime,onPremisesSyncEnabled,operatingSystem,operatingSystemVersion,"
        "physicalIds,profileType,registrationDateTime,sourceType,systemLabels,trustType"
    )
    ENDPOINT_KEY = "devices"

    def __init__(self, extractor: GraphDataExtractor):
        super().__init__(extractor)

    # Transform

    def transform(self, data: list[dict]) -> list[dict]:
        transformed = []

        for record in data:
            is_deleted = "@removed" in record

            entry: dict = {
                "id": record.get("id"),
                "is_deleted": is_deleted,
                "deleted_at": datetime.now(timezone.utc) if is_deleted else _MISSING,
                "raw_json": _strip_meta(record),
            }

            for col, graph_key in FIELD_MAP:
                val = record.get(graph_key, _MISSING)

                # Parse ISO datetime strings → Python datetime objects
                if val not in (_MISSING, None) and col in DATETIME_COLS:
                    val = datetime.fromisoformat(val.replace("Z", "+00:00"))

                entry[col] = val

            transformed.append(entry)

        logger.info(
            f"[{self.ENDPOINT_KEY}] Transformed {len(transformed)} records "
            f"({sum(1 for r in transformed if r['is_deleted'])} deletes)"
        )
        return transformed

    # Load

    def load(self, data: list[dict], session: Session) -> None:
        now = datetime.now(timezone.utc)
        upserted = 0
        deleted = 0

        # Deduplicate + merge
        deduped: dict[str, dict] = {}
        for record in data:
            did = record["id"]
            if did not in deduped:
                deduped[did] = record
            else:
                if record["is_deleted"]:
                    deduped[did] = record
                else:
                    existing = deduped[did]
                    for key, val in record.items():
                        if val is not _MISSING:
                            existing[key] = val

        if len(deduped) < len(data):
            logger.warning(
                f"[{self.ENDPOINT_KEY}] Deduplicated {len(data) - len(deduped)} "
                f"duplicate record(s) before load"
            )

        for record in deduped.values():
            device = session.get(Device, record["id"])
            is_new = device is None

            if device is None:
                device = Device(id=record["id"])
                session.add(device)

            # Soft delete
            if record["is_deleted"]:
                device.is_deleted = True
                device.deleted_at = record["deleted_at"]
                device.synced_at = now
                deleted += 1
                continue

            # Scalar fields
            for col, _ in FIELD_MAP:
                val = record[col]
                if val is not _MISSING:
                    setattr(device, col, val)

            # raw_json
            if record["raw_json"] is not None:
                existing = device.raw_json or {}
                device.raw_json = {**existing, **record["raw_json"]}
            elif is_new:
                device.raw_json = None

            device.is_deleted = False
            device.deleted_at = None
            device.synced_at = now
            upserted += 1

        # DO NOT call session.commit() — context manager owns the transaction
        logger.info(
            f"[{self.ENDPOINT_KEY}] Load complete — "
            f"{upserted} upserted, {deleted} soft-deleted"
        )

    # FK Backfill

    def backfill_user_ids(self, session: Session) -> None:
        """
        Fetches user→registeredDevices mapping from Graph and
        updates user_id FK on all matching device rows in the DB.
        Called separately in main.py AFTER both pipelines commit.
        """
        logger.info(f"[{self.ENDPOINT_KEY}] Starting user_id backfill")

        mapping = build_user_device_mapping()
        if not mapping:
            logger.warning(f"[{self.ENDPOINT_KEY}] Empty mapping — nothing to backfill")
            return

        # Pre-fetch all user IDs from the mapping that actually exist in the DB
        # Prevents FK constraint violations if a user was deleted before devices synced
        candidate_user_ids = set(mapping.values())
        existing_user_ids = {
            row[0]
            for row in session.query(User.id)
            .filter(User.id.in_(candidate_user_ids))
            .all()
        }

        missing_users = candidate_user_ids - existing_user_ids
        if missing_users:
            logger.warning(
                f"[{self.ENDPOINT_KEY}] {len(missing_users)} user ID(s) from mapping "
                f"not found in DB — their devices will be skipped"
            )

        updated = 0
        skipped_no_device = 0
        skipped_no_user = 0

        for device_id, user_id in mapping.items():
            # Guard 1 — user must exist in DB before we set the FK
            if user_id not in existing_user_ids:
                skipped_no_user += 1
                continue

            # Guard 2 — device must exist in DB
            device = session.get(Device, device_id)
            if device is None:
                skipped_no_device += 1
                continue

            if device.user_id != user_id:
                device.user_id = user_id
                updated += 1

        logger.info(
            f"[{self.ENDPOINT_KEY}] Backfill complete — "
            f"{updated} updated | "
            f"{skipped_no_device} devices not in DB | "
            f"{skipped_no_user} skipped (user not in DB)"
        )


# Helpers
def _strip_meta(record: dict) -> dict | None:
    """Keep only unmapped fields (e.g. extensionAttributes) for raw_json storage."""
    extra = {k: v for k, v in record.items() if k not in MAPPED_FIELDS}
    return extra if extra else None
