import ast
import csv
import json
import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from db.models import User, Device

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
# Parsers                                                              #
# ------------------------------------------------------------------ #

def _parse_dt(val: str | None) -> datetime | None:
    if not val or val.strip() in ("", "None"):
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(f"Could not parse datetime: {val}")
        return None


def _parse_bool(val: str | None) -> bool | None:
    if val is None or val.strip() in ("", "None"):
        return None
    return val.strip().lower() in ("true", "1", "yes")


def _parse_registered_devices(raw: str) ->list[str]:
    """
    Parses the registeredDevices column.
    Format: pipe-separated Python dict strings.
    Returns { device_graph_id -> user_id } — populated by caller after user_id is known.
    Returns list of device graph IDs associated with this user.
    """
    device_ids = []
    if not raw or raw.strip() in ("", "None"):
        return device_ids

    for entry in raw.split("|"):
        entry = entry.strip()
        if not entry:
            continue
        try:
            device_dict = ast.literal_eval(entry)
            graph_id = device_dict.get("id")
            if graph_id:
                device_ids.append(graph_id)
        except (ValueError, SyntaxError) as e:
            logger.warning(f"Could not parse registeredDevices entry: {entry[:80]} — {e}")

    return device_ids


def _pack_extension_attrs(row: dict) -> str | None:
    """
    Collects all extensionAttributes_* columns into a JSON dict.
    Only includes non-empty values.
    Returns JSON string or None if all are empty.
    """
    ext_prefix = "extensionAttributes_"
    ext_dict = {
        k.replace(ext_prefix, ""): v.strip()
        for k, v in row.items()
        if k.startswith(ext_prefix) and v and v.strip() not in ("", "None")
    }
    return json.dumps(ext_dict, ensure_ascii=False) if ext_dict else None

# ------------------------------------------------------------------ #
# User import                                                          #
# ------------------------------------------------------------------ #

def import_users(filepath: str, session: Session) -> dict[str, str]:
    """
    Reads users CSV and upserts into the users table.

    Returns:
        device_to_user: dict mapping device graph ID → user ID
    """
    logger.info(f"Importing users from: {filepath}")
    device_to_user: dict[str, str] = {}
    count = 0

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            user_id = row["id"]

            # Build device → user mapping from registeredDevices
            device_ids = _parse_registered_devices(row.get("registeredDevices", ""))
            for dev_id in device_ids:
                device_to_user[dev_id] = user_id

            user = User(
                id=user_id,
                business_phones=row.get("businessPhones"),
                display_name=row.get("displayName"),
                given_name=row.get("givenName"),
                surname=row.get("surname"),
                job_title=row.get("jobTitle"),
                mail=row.get("mail"),
                mobile_phone=row.get("mobilePhone"),
                office_location=row.get("officeLocation"),
                preferred_language=row.get("preferredLanguage"),
                user_principal_name=row.get("userPrincipalName"),
                raw_json=None,
                synced_at=datetime.now(timezone.utc),
            )
            session.merge(user)
            count += 1

    session.flush()
    logger.info(f"Upserted {count} users | Mapped {len(device_to_user)} devices to users")
    return device_to_user


# ------------------------------------------------------------------ #
# Device import                                                        #
# ------------------------------------------------------------------ #

def import_devices(filepath: str, session: Session, device_to_user: dict[str, str]):
    """
    Reads devices CSV and upserts into the devices table.
    Uses device_to_user map to set user_id FK.
    extensionAttributes_* columns are packed into raw_json.
    """
    logger.info(f"Importing devices from: {filepath}")
    count = 0
    unmatched = 0

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            device_graph_id = row["id"]
            user_id = device_to_user.get(device_graph_id)

            if not user_id:
                unmatched += 1
                logger.debug(f"Device {device_graph_id} has no matching user — user_id will be NULL")

            device = Device(
                id=device_graph_id,
                user_id=user_id,

                account_enabled=_parse_bool(row.get("accountEnabled")),
                alternative_security_ids=row.get("alternativeSecurityIds"),
                approximate_last_signin_datetime=_parse_dt(row.get("approximateLastSignInDateTime")),
                compliance_expiration_datetime=_parse_dt(row.get("complianceExpirationDateTime")),
                created_datetime=_parse_dt(row.get("createdDateTime")),
                deleted_datetime=_parse_dt(row.get("deletedDateTime")),

                device_category=row.get("deviceCategory"),
                device_id=row.get("deviceId"),
                device_metadata=row.get("deviceMetadata"),
                device_ownership=row.get("deviceOwnership"),
                device_version=row.get("deviceVersion"),
                display_name=row.get("displayName"),
                domain_name=row.get("domainName"),
                enrollment_profile_name=row.get("enrollmentProfileName"),
                enrollment_type=row.get("enrollmentType"),

                external_source_name=row.get("externalSourceName"),
                is_compliant=_parse_bool(row.get("isCompliant")),
                is_managed=_parse_bool(row.get("isManaged")),
                is_rooted=_parse_bool(row.get("isRooted")),
                management_type=row.get("managementType"),
                manufacturer=row.get("manufacturer"),
                mdm_app_id=row.get("mdmAppId"),
                model=row.get("model"),

                on_premises_last_sync_datetime=_parse_dt(row.get("onPremisesLastSyncDateTime")),
                on_premises_sync_enabled=_parse_bool(row.get("onPremisesSyncEnabled")),

                operating_system=row.get("operatingSystem"),
                operating_system_version=row.get("operatingSystemVersion"),

                physical_ids=row.get("physicalIds"),
                profile_type=row.get("profileType"),
                registration_datetime=_parse_dt(row.get("registrationDateTime")),
                source_type=row.get("sourceType"),
                system_labels=row.get("systemLabels"),
                trust_type=row.get("trustType"),

                raw_json=_pack_extension_attrs(row),
                synced_at=datetime.now(timezone.utc),
            )
            session.merge(device)
            count += 1

    session.flush()
    logger.info(f"Upserted {count} devices | {unmatched} devices had no matching user")


# ------------------------------------------------------------------ #
# Orchestrator                                                         #
# ------------------------------------------------------------------ #

def run_import(users_csv: str, devices_csv: str, session: Session):
    """
    Full import orchestration — users first, then devices.
    Session commit is handled by the caller.
    """
    logger.info("=== Starting full import ===")
    device_to_user = import_users(users_csv, session)
    import_devices(devices_csv, session, device_to_user)
    logger.info("=== Import complete ===")