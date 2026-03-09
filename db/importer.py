import ast
import csv
import json
import logging
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.orm import Session
from db.models import ManagedDevice, User, Device

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# Parsers


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


def _parse_registered_devices(raw: str) -> list[str]:
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
            logger.warning(
                f"Could not parse registeredDevices entry: {entry[:80]} — {e}"
            )

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


MANAGED_DEVICE_CM_FEATURES_PREFIX = "configurationManagerClientEnabledFeatures"


def _pack_managed_device_raw_json(row: dict) -> str | None:
    """
    Packs configurationManagerClientEnabledFeatures_* columns,
    deviceActionResults and deviceHealthAttestationState into raw_json.
    """
    keys_to_pack = {
        "deviceActionResults",
        "deviceHealthAttestationState",
        "configurationManagerClientEnabledFeatures",
    }
    packed = {}

    for k, v in row.items():
        is_cm_feature = k.startswith(MANAGED_DEVICE_CM_FEATURES_PREFIX)
        is_extra_field = k in keys_to_pack
        if (is_cm_feature or is_extra_field) and v and v.strip() not in ("", "None"):
            packed[k] = v.strip()

    return json.dumps(packed, ensure_ascii=False) if packed else None


# User import


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
    logger.info(
        f"Upserted {count} users | Mapped {len(device_to_user)} devices to users"
    )
    return device_to_user


# Device import


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
                logger.debug(
                    f"Device {device_graph_id} has no matching user — user_id will be NULL"
                )

            device = Device(
                id=device_graph_id,
                user_id=user_id,
                account_enabled=_parse_bool(row.get("accountEnabled")),
                alternative_security_ids=row.get("alternativeSecurityIds"),
                approximate_last_signin_datetime=_parse_dt(
                    row.get("approximateLastSignInDateTime")
                ),
                compliance_expiration_datetime=_parse_dt(
                    row.get("complianceExpirationDateTime")
                ),
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
                on_premises_last_sync_datetime=_parse_dt(
                    row.get("onPremisesLastSyncDateTime")
                ),
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


def import_managed_devices(filepath: str, session: Session):
    logger.info(f"Importing managed devices from: {filepath}")

    # Build set of known user IDs to validate FK before insert
    valid_user_ids = set(row[0] for row in session.execute(select(User.id)).all())
    logger.info(f"Found {len(valid_user_ids)} valid users for FK validation")

    count = 0
    unmatched = 0

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_user_id = row.get("userId", "").strip()

            # Only set user_id if it actually exists in users table
            user_id = raw_user_id if raw_user_id in valid_user_ids else None
            if not user_id:
                unmatched += 1
                logger.debug(
                    f"ManagedDevice {row.get('id')} userId '{raw_user_id}' "
                    f"not found in users table — setting NULL"
                )

            managed_device = ManagedDevice(
                id=row["id"],
                user_id=user_id,
                azure_ad_device_id=row.get("azureADDeviceId"),  # plain string, no FK
                device_name=row.get("deviceName"),
                managed_device_name=row.get("managedDeviceName"),
                serial_number=row.get("serialNumber"),
                imei=row.get("imei"),
                meid=row.get("meid"),
                iccid=row.get("iccid"),
                udid=row.get("udid"),
                email_address=row.get("emailAddress"),
                user_display_name=row.get("userDisplayName"),
                user_principal_name=row.get("userPrincipalName"),
                operating_system=row.get("operatingSystem"),
                os_version=row.get("osVersion"),
                manufacturer=row.get("manufacturer"),
                model=row.get("model"),
                phone_number=row.get("phoneNumber"),
                wi_fi_mac_address=row.get("wiFiMacAddress"),
                ethernet_mac_address=row.get("ethernetMacAddress"),
                free_storage_space_in_bytes=row.get("freeStorageSpaceInBytes"),
                total_storage_space_in_bytes=row.get("totalStorageSpaceInBytes"),
                physical_memory_in_bytes=row.get("physicalMemoryInBytes"),
                android_security_patch_level=row.get("androidSecurityPatchLevel"),
                device_enrollment_type=row.get("deviceEnrollmentType"),
                enrollment_profile_name=row.get("enrollmentProfileName"),
                enrolled_datetime=_parse_dt(row.get("enrolledDateTime")),
                last_sync_datetime=_parse_dt(row.get("lastSyncDateTime")),
                management_agent=row.get("managementAgent"),
                management_state=row.get("managementState"),
                management_certificate_expiration_date=_parse_dt(
                    row.get("managementCertificateExpirationDate")
                ),
                managed_device_owner_type=row.get("managedDeviceOwnerType"),
                device_registration_state=row.get("deviceRegistrationState"),
                device_category_display_name=row.get("deviceCategoryDisplayName"),
                azure_ad_registered=_parse_bool(row.get("azureADRegistered")),
                compliance_state=row.get("complianceState"),
                compliance_grace_period_expiration_datetime=_parse_dt(
                    row.get("complianceGracePeriodExpirationDateTime")
                ),
                is_encrypted=_parse_bool(row.get("isEncrypted")),
                is_supervised=_parse_bool(row.get("isSupervised")),
                jail_broken=row.get("jailBroken"),
                partner_reported_threat_state=row.get("partnerReportedThreatState"),
                activation_lock_bypass_code=row.get("activationLockBypassCode"),
                eas_activated=_parse_bool(row.get("easActivated")),
                eas_activation_datetime=_parse_dt(row.get("easActivationDateTime")),
                eas_device_id=row.get("easDeviceId"),
                exchange_access_state=row.get("exchangeAccessState"),
                exchange_access_state_reason=row.get("exchangeAccessStateReason"),
                exchange_last_successful_sync_datetime=_parse_dt(
                    row.get("exchangeLastSuccessfulSyncDateTime")
                ),
                notes=row.get("notes"),
                require_user_enrollment_approval=_parse_bool(
                    row.get("requireUserEnrollmentApproval")
                ),
                remote_assistance_session_url=row.get("remoteAssistanceSessionUrl"),
                remote_assistance_session_error_details=row.get(
                    "remoteAssistanceSessionErrorDetails"
                ),
                subscriber_carrier=row.get("subscriberCarrier"),
                raw_json=_pack_managed_device_raw_json(row),
                synced_at=datetime.now(timezone.utc),
            )
            session.merge(managed_device)
            count += 1

    session.flush()
    logger.info(
        f"Upserted {count} managed devices | {unmatched} had unresolvable user_id"
    )


# Orchestrator


def run_import(
    users_csv: str, devices_csv: str, managed_devices_csv: str, session: Session
):
    """
    Full import orchestration — users first, then devices.
    Session commit is handled by the caller.
    """
    logger.info("=== Starting full import ===")
    device_to_user = import_users(users_csv, session)
    import_devices(devices_csv, session, device_to_user)
    import_managed_devices(managed_devices_csv, session)
    logger.info("=== Import complete ===")
