from sqlalchemy import String, Boolean, DateTime, Text, ForeignKey, Index, JSON
from sqlalchemy.orm import relationship, Mapped, mapped_column
from db.database import Base
from datetime import datetime, timezone


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    display_name: Mapped[str | None] = mapped_column(String)
    given_name: Mapped[str | None] = mapped_column(String)
    surname: Mapped[str | None] = mapped_column(String)
    mail: Mapped[str | None] = mapped_column(String)
    user_principal_name: Mapped[str | None] = mapped_column(String)
    job_title: Mapped[str | None] = mapped_column(String)
    office_location: Mapped[str | None] = mapped_column(String)
    business_phones: Mapped[str | None] = mapped_column(Text)
    mobile_phone: Mapped[str | None] = mapped_column(String)
    preferred_language: Mapped[str | None] = mapped_column(String)

    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    devices = relationship("Device", back_populates="user")
    managed_devices = relationship("ManagedDevice", back_populates="user")

    __table_args__ = (
        Index("ix_users_mail", "mail"),
        Index("ix_users_upn", "user_principal_name"),
        Index("ix_users_display_name", "display_name"),
    )


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id"))

    account_enabled: Mapped[bool | None] = mapped_column(Boolean)
    alternative_security_ids: Mapped[list | None] = mapped_column(
        JSON
    )  # JSON handles arrays perfectly
    approximate_last_signin_datetime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    compliance_expiration_datetime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    created_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    device_category: Mapped[str | None] = mapped_column(String)
    device_id: Mapped[str | None] = mapped_column(String)
    device_metadata: Mapped[str | None] = mapped_column(String)
    device_ownership: Mapped[str | None] = mapped_column(String)
    device_version: Mapped[str | None] = mapped_column(String)
    display_name: Mapped[str | None] = mapped_column(String)
    domain_name: Mapped[str | None] = mapped_column(String)
    enrollment_profile_name: Mapped[str | None] = mapped_column(String)
    enrollment_type: Mapped[str | None] = mapped_column(String)

    external_source_name: Mapped[str | None] = mapped_column(String)
    is_compliant: Mapped[bool | None] = mapped_column(Boolean)
    is_managed: Mapped[bool | None] = mapped_column(Boolean)
    is_rooted: Mapped[bool | None] = mapped_column(Boolean)
    management_type: Mapped[str | None] = mapped_column(String)
    manufacturer: Mapped[str | None] = mapped_column(String)
    mdm_app_id: Mapped[str | None] = mapped_column(String)
    model: Mapped[str | None] = mapped_column(String)

    on_premises_last_sync_datetime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    on_premises_sync_enabled: Mapped[bool | None] = mapped_column(Boolean)

    operating_system: Mapped[str | None] = mapped_column(String)
    operating_system_version: Mapped[str | None] = mapped_column(String)

    physical_ids: Mapped[list | None] = mapped_column(JSON)
    profile_type: Mapped[str | None] = mapped_column(String)
    registration_datetime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    source_type: Mapped[str | None] = mapped_column(String)
    system_labels: Mapped[list | None] = mapped_column(JSON)
    trust_type: Mapped[str | None] = mapped_column(String)

    # Extension attributes and anything else not mapped will live here
    raw_json: Mapped[dict | None] = mapped_column(JSON)

    # Pipeline specific metadata
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    user = relationship("User", back_populates="devices")

    __table_args__ = (
        Index("ix_devices_user_id", "user_id"),
        Index("ix_devices_os", "operating_system"),
        Index("ix_devices_is_compliant", "is_compliant"),
        Index("ix_devices_display_name", "display_name"),
    )


class ManagedDevice(Base):
    __tablename__ = "managed_devices"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str | None] = mapped_column(String, ForeignKey("users.id"))

    # Core identity
    device_name: Mapped[str | None] = mapped_column(String)
    managed_device_name: Mapped[str | None] = mapped_column(String)
    azure_ad_device_id: Mapped[str | None] = mapped_column(String)
    serial_number: Mapped[str | None] = mapped_column(String)
    imei: Mapped[str | None] = mapped_column(String)
    meid: Mapped[str | None] = mapped_column(String)
    iccid: Mapped[str | None] = mapped_column(String)
    udid: Mapped[str | None] = mapped_column(String)
    email_address: Mapped[str | None] = mapped_column(String)
    user_display_name: Mapped[str | None] = mapped_column(String)
    user_principal_name: Mapped[str | None] = mapped_column(String)

    # OS & hardware
    operating_system: Mapped[str | None] = mapped_column(String)
    os_version: Mapped[str | None] = mapped_column(String)
    manufacturer: Mapped[str | None] = mapped_column(String)
    model: Mapped[str | None] = mapped_column(String)
    phone_number: Mapped[str | None] = mapped_column(String)
    wi_fi_mac_address: Mapped[str | None] = mapped_column(String)
    ethernet_mac_address: Mapped[str | None] = mapped_column(String)
    free_storage_space_in_bytes: Mapped[int | None] = mapped_column(
        String
    )  # Can exceed int32
    total_storage_space_in_bytes: Mapped[int | None] = mapped_column(
        String
    )  # Store as String — too large for SQLite Integer
    physical_memory_in_bytes: Mapped[int | None] = mapped_column(String)
    android_security_patch_level: Mapped[str | None] = mapped_column(String)

    # Enrollment & management
    device_enrollment_type: Mapped[str | None] = mapped_column(String)
    enrollment_profile_name: Mapped[str | None] = mapped_column(String)
    enrolled_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_sync_datetime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    management_agent: Mapped[str | None] = mapped_column(String)
    management_state: Mapped[str | None] = mapped_column(String)
    management_certificate_expiration_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    managed_device_owner_type: Mapped[str | None] = mapped_column(String)
    device_registration_state: Mapped[str | None] = mapped_column(String)
    device_category_display_name: Mapped[str | None] = mapped_column(String)
    azure_ad_registered: Mapped[bool | None] = mapped_column(Boolean)

    # Compliance
    compliance_state: Mapped[str | None] = mapped_column(String)
    compliance_grace_period_expiration_datetime: Mapped[datetime | None] = (
        mapped_column(DateTime(timezone=True))
    )
    is_encrypted: Mapped[bool | None] = mapped_column(Boolean)
    is_supervised: Mapped[bool | None] = mapped_column(Boolean)
    jail_broken: Mapped[str | None] = mapped_column(String)
    partner_reported_threat_state: Mapped[str | None] = mapped_column(String)
    activation_lock_bypass_code: Mapped[str | None] = mapped_column(String)

    # Exchange / EAS
    eas_activated: Mapped[bool | None] = mapped_column(Boolean)
    eas_activation_datetime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    eas_device_id: Mapped[str | None] = mapped_column(String)
    exchange_access_state: Mapped[str | None] = mapped_column(String)
    exchange_access_state_reason: Mapped[str | None] = mapped_column(String)
    exchange_last_successful_sync_datetime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

    # Remote / misc
    notes: Mapped[str | None] = mapped_column(Text)
    require_user_enrollment_approval: Mapped[bool | None] = mapped_column(Boolean)
    remote_assistance_session_url: Mapped[str | None] = mapped_column(String)
    remote_assistance_session_error_details: Mapped[str | None] = mapped_column(String)
    subscriber_carrier: Mapped[str | None] = mapped_column(String)

    # raw_json — catches configurationManagerClientEnabledFeatures,
    # deviceActionResults, deviceHealthAttestationState and anything else unmapped
    raw_json: Mapped[dict | None] = mapped_column(JSON)

    # Pipeline metadata
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    synced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    user = relationship("User", back_populates="managed_devices")

    __table_args__ = (
        Index("ix_managed_devices_user_id", "user_id"),
        Index("ix_managed_devices_os", "operating_system"),
        Index("ix_managed_devices_compliance", "compliance_state"),
        Index("ix_managed_devices_last_sync", "last_sync_datetime"),
        Index(
            "ix_managed_devices_azure_ad_did", "azure_ad_device_id"
        ),  # useful for joining to devices table
    )
