from sqlalchemy import Column, String, Boolean, DateTime, Text, ForeignKey, Index
from sqlalchemy.orm import relationship
from db.database import Base  


class User(Base):
    __tablename__ = "users"

    id                  = Column(String, primary_key=True)
    business_phones     = Column(Text)           # comma-separated or JSON list
    display_name        = Column(String)
    given_name          = Column(String)
    surname             = Column(String)
    job_title           = Column(String)
    mail                = Column(String)
    mobile_phone        = Column(String)
    office_location     = Column(String)
    preferred_language  = Column(String)
    user_principal_name = Column(String)

    raw_json            = Column(Text)           # column for any addition attributes introduced in the future.
    synced_at           = Column(DateTime)

    devices = relationship("Device", back_populates="user")
    managed_devices = relationship("ManagedDevice", back_populates="user") 

    __table_args__ = (
        Index("ix_users_mail", "mail"),
        Index("ix_users_upn", "user_principal_name"),
        Index("ix_users_display_name", "display_name"),
    )


class Device(Base):
    __tablename__ = "devices"

    id                              = Column(String, primary_key=True)
    user_id                         = Column(String, ForeignKey("users.id"), nullable=True)

    account_enabled                 = Column(Boolean)
    alternative_security_ids        = Column(Text)
    approximate_last_signin_datetime = Column(DateTime)
    compliance_expiration_datetime  = Column(DateTime)
    created_datetime                = Column(DateTime)
    deleted_datetime                = Column(DateTime)

    device_category                 = Column(String)
    device_id                       = Column(String)
    device_metadata                 = Column(Text)
    device_ownership                = Column(String)
    device_version                  = Column(String)
    display_name                    = Column(String)
    domain_name                     = Column(String)
    enrollment_profile_name         = Column(String)
    enrollment_type                 = Column(String)

    external_source_name            = Column(String)
    is_compliant                    = Column(Boolean)
    is_managed                      = Column(Boolean)
    is_rooted                       = Column(Boolean)
    management_type                 = Column(String)
    manufacturer                    = Column(String)
    mdm_app_id                      = Column(String)
    model                           = Column(String)

    on_premises_last_sync_datetime  = Column(DateTime)
    on_premises_sync_enabled        = Column(Boolean)

    operating_system                = Column(String)
    operating_system_version        = Column(String)

    physical_ids                    = Column(Text)
    profile_type                    = Column(String)
    registration_datetime           = Column(DateTime)
    source_type                     = Column(String)
    system_labels                   = Column(Text)
    trust_type                      = Column(String)
    # extensionAttributes_* are NOT individual columns —
    # all 15 are stored as a JSON dict here
    raw_json                        = Column(Text)
    synced_at                       = Column(DateTime)

    user = relationship("User", back_populates="devices")

    __table_args__ = (
        Index("ix_devices_user_id", "user_id"),
        Index("ix_devices_os", "operating_system"),
        Index("ix_devices_is_compliant", "is_compliant"),
        Index("ix_devices_display_name", "display_name"),
    )

class ManagedDevice(Base):
    __tablename__ = "managed_devices"

    id                                      = Column(String, primary_key=True)

    # FK to users
    user_id                                 = Column(String, ForeignKey("users.id"), nullable=True)

    # Core identity
    device_name                             = Column(String)
    managed_device_name                     = Column(String)
    azure_ad_device_id                      = Column(String)
    serial_number                           = Column(String)
    imei                                    = Column(String)
    meid                                    = Column(String)
    iccid                                   = Column(String)
    udid                                    = Column(String)
    email_address                           = Column(String)
    user_display_name                       = Column(String)
    user_principal_name                     = Column(String)

    # OS & hardware
    operating_system                        = Column(String)
    os_version                              = Column(String)
    manufacturer                            = Column(String)
    model                                   = Column(String)
    phone_number                            = Column(String)
    wi_fi_mac_address                       = Column(String)
    ethernet_mac_address                    = Column(String)
    free_storage_space_in_bytes             = Column(String)
    total_storage_space_in_bytes            = Column(String)
    physical_memory_in_bytes                = Column(String)
    android_security_patch_level            = Column(String)

    # Enrollment & management
    device_enrollment_type                  = Column(String)
    enrollment_profile_name                 = Column(String)
    enrolled_datetime                       = Column(DateTime)
    last_sync_datetime                      = Column(DateTime)
    management_agent                        = Column(String)
    management_state                        = Column(String)
    management_certificate_expiration_date  = Column(DateTime)
    managed_device_owner_type               = Column(String)
    device_registration_state               = Column(String)
    device_category_display_name            = Column(String)
    azure_ad_registered                     = Column(Boolean)

    # Compliance
    compliance_state                        = Column(String)
    compliance_grace_period_expiration_datetime = Column(DateTime)
    is_encrypted                            = Column(Boolean)
    is_supervised                           = Column(Boolean)
    jail_broken                             = Column(String)   # "Unknown"/"False"/"True"
    partner_reported_threat_state           = Column(String)
    activation_lock_bypass_code             = Column(String)

    # Exchange / EAS
    eas_activated                           = Column(Boolean)
    eas_activation_datetime                 = Column(DateTime)
    eas_device_id                           = Column(String)
    exchange_access_state                   = Column(String)
    exchange_access_state_reason            = Column(String)
    exchange_last_successful_sync_datetime  = Column(DateTime)

    # Remote / misc
    notes                                   = Column(Text)
    require_user_enrollment_approval        = Column(Boolean)
    remote_assistance_session_url           = Column(String)
    remote_assistance_session_error_details = Column(String)
    subscriber_carrier                      = Column(String)

    # raw_json — catches configurationManagerClientEnabledFeatures_*
    # deviceActionResults, deviceHealthAttestationState
    raw_json                                = Column(Text)
    synced_at                               = Column(DateTime)

    # Relationship
    user = relationship("User", back_populates="managed_devices")

    __table_args__ = (
        Index("ix_managed_devices_user_id", "user_id"),
        Index("ix_managed_devices_os", "operating_system"),
        Index("ix_managed_devices_compliance_state", "compliance_state"),
        Index("ix_managed_devices_last_sync", "last_sync_datetime"),
    )
