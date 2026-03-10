import logging
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from db.models import ManagedDevice, User
from pipelines.graph_base_pipline import GraphBasePipeline
from data_extraction.graph_data_extractor import GraphDataExtractor
from utils import dump_to_json

logger = logging.getLogger(__name__)

_MISSING = object()

MAPPED_FIELDS = {
    "id",
    "userId",
    "deviceName",
    "managedDeviceName",
    "azureADDeviceId",
    "serialNumber",
    "imei",
    "meid",
    "iccid",
    "udid",
    "emailAddress",
    "userDisplayName",
    "userPrincipalName",
    "operatingSystem",
    "osVersion",
    "manufacturer",
    "model",
    "phoneNumber",
    "wiFiMacAddress",
    "ethernetMacAddress",
    "freeStorageSpaceInBytes",
    "totalStorageSpaceInBytes",
    "physicalMemoryInBytes",
    "androidSecurityPatchLevel",
    "deviceEnrollmentType",
    "enrollmentProfileName",
    "enrolledDateTime",
    "lastSyncDateTime",
    "managementAgent",
    "managementState",
    "managementCertificateExpirationDate",
    "managedDeviceOwnerType",
    "deviceRegistrationState",
    "deviceCategoryDisplayName",
    "azureADRegistered",
    "complianceState",
    "complianceGracePeriodExpirationDateTime",
    "isEncrypted",
    "isSupervised",
    "jailBroken",
    "partnerReportedThreatState",
    "activationLockBypassCode",
    "easActivated",
    "easActivationDateTime",
    "easDeviceId",
    "exchangeAccessState",
    "exchangeAccessStateReason",
    "exchangeLastSuccessfulSyncDateTime",
    "notes",
    "requireUserEnrollmentApproval",
    "remoteAssistanceSessionUrl",
    "remoteAssistanceSessionErrorDetails",
    "subscriberCarrier",
    "@odata.type",
    "@odata.id",
    "@odata.context",
}

FIELD_MAP: list[tuple[str, str]] = [
    ("user_id", "userId"),
    ("device_name", "deviceName"),
    ("managed_device_name", "managedDeviceName"),
    ("azure_ad_device_id", "azureADDeviceId"),
    ("serial_number", "serialNumber"),
    ("imei", "imei"),
    ("meid", "meid"),
    ("iccid", "iccid"),
    ("udid", "udid"),
    ("email_address", "emailAddress"),
    ("user_display_name", "userDisplayName"),
    ("user_principal_name", "userPrincipalName"),
    ("operating_system", "operatingSystem"),
    ("os_version", "osVersion"),
    ("manufacturer", "manufacturer"),
    ("model", "model"),
    ("phone_number", "phoneNumber"),
    ("wi_fi_mac_address", "wiFiMacAddress"),
    ("ethernet_mac_address", "ethernetMacAddress"),
    ("free_storage_space_in_bytes", "freeStorageSpaceInBytes"),
    ("total_storage_space_in_bytes", "totalStorageSpaceInBytes"),
    ("physical_memory_in_bytes", "physicalMemoryInBytes"),
    ("android_security_patch_level", "androidSecurityPatchLevel"),
    ("device_enrollment_type", "deviceEnrollmentType"),
    ("enrollment_profile_name", "enrollmentProfileName"),
    ("enrolled_datetime", "enrolledDateTime"),
    ("last_sync_datetime", "lastSyncDateTime"),
    ("management_agent", "managementAgent"),
    ("management_state", "managementState"),
    ("management_certificate_expiration_date", "managementCertificateExpirationDate"),
    ("managed_device_owner_type", "managedDeviceOwnerType"),
    ("device_registration_state", "deviceRegistrationState"),
    ("device_category_display_name", "deviceCategoryDisplayName"),
    ("azure_ad_registered", "azureADRegistered"),
    ("compliance_state", "complianceState"),
    (
        "compliance_grace_period_expiration_datetime",
        "complianceGracePeriodExpirationDateTime",
    ),
    ("is_encrypted", "isEncrypted"),
    ("is_supervised", "isSupervised"),
    ("jail_broken", "jailBroken"),
    ("partner_reported_threat_state", "partnerReportedThreatState"),
    ("activation_lock_bypass_code", "activationLockBypassCode"),
    ("eas_activated", "easActivated"),
    ("eas_activation_datetime", "easActivationDateTime"),
    ("eas_device_id", "easDeviceId"),
    ("exchange_access_state", "exchangeAccessState"),
    ("exchange_access_state_reason", "exchangeAccessStateReason"),
    ("exchange_last_successful_sync_datetime", "exchangeLastSuccessfulSyncDateTime"),
    ("notes", "notes"),
    ("require_user_enrollment_approval", "requireUserEnrollmentApproval"),
    ("remote_assistance_session_url", "remoteAssistanceSessionUrl"),
    ("remote_assistance_session_error_details", "remoteAssistanceSessionErrorDetails"),
    ("subscriber_carrier", "subscriberCarrier"),
]

DATETIME_COLS = {
    "enrolled_datetime",
    "last_sync_datetime",
    "management_certificate_expiration_date",
    "compliance_grace_period_expiration_datetime",
    "eas_activation_datetime",
    "exchange_last_successful_sync_datetime",
    "exchange_last_successful_sync_datetime",
}


class ManagedDevicesPipeline(GraphBasePipeline):
    # Intune managedDevices has NO delta support — always full sync
    ENDPOINT = "deviceManagement/managedDevices?$top=999"
    ENDPOINT_KEY = "managed_devices"

    def __init__(self, extractor: GraphDataExtractor):
        super().__init__(extractor)

    # Transform

    def transform(self, data: list[dict]) -> list[dict]:
        transformed = []

        for record in data:
            entry: dict = {
                "id": record.get("id"),
                "raw_json": _strip_meta(record),
            }

            for col, graph_key in FIELD_MAP:
                val = record.get(graph_key, _MISSING)

                # Parse ISO datetime strings → Python datetime objects
                if val not in (_MISSING, None) and col in DATETIME_COLS:
                    val = datetime.fromisoformat(val.replace("Z", "+00:00"))

                entry[col] = val

            transformed.append(entry)

        logger.info(f"[{self.ENDPOINT_KEY}] Transformed {len(transformed)} records")
        return transformed

    # Load

    def load(self, data: list[dict], session: Session) -> None:
        now = datetime.now(timezone.utc)

        # Truncate
        session.query(ManagedDevice).delete()
        logger.info(
            f"[{self.ENDPOINT_KEY}] Table truncated — reinserting {len(data)} records"
        )

        # FK safety
        candidate_user_ids = {
            r["user_id"] for r in data if r["user_id"] not in (_MISSING, None)
        }
        valid_user_ids: set[str] = set()
        if candidate_user_ids:
            valid_user_ids = {
                row[0]
                for row in session.query(User.id)
                .filter(User.id.in_(candidate_user_ids))
                .all()
            }

        # Bulk insert
        for record in data:
            device = ManagedDevice(id=record["id"])

            for col, _ in FIELD_MAP:
                if col == "user_id":
                    continue
                val = record[col]
                if val is not _MISSING:
                    setattr(device, col, val)

            # user_id FK
            user_id_val = record["user_id"]
            if user_id_val not in (_MISSING, None) and user_id_val in valid_user_ids:
                device.user_id = user_id_val
            else:
                device.user_id = None

            # raw_json
            device.raw_json = record["raw_json"]
            device.synced_at = now

            session.add(device)

        logger.info(
            f"[{self.ENDPOINT_KEY}] Load complete — {len(data)} records inserted"
        )


# Helpers


def _strip_meta(record: dict) -> dict | None:
    """Store only unmapped fields in raw_json."""
    extra = {k: v for k, v in record.items() if k not in MAPPED_FIELDS}
    return extra if extra else None
