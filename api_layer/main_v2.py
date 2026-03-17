from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from db.database import Database
from db.models import User, Device, ManagedDevice

app = FastAPI()

db_instance = Database("sqlite:///data.db")


def get_db():
    with db_instance.get_session() as session:
        yield session


# USERS
@app.get("/users/all")
def get_all_users(db: Session = Depends(get_db)):

    users = db.query(
        User.id,
        User.display_name,
        User.job_title,
        User.mail,
        User.mobile_phone,
        User.office_location,
    ).all()

    return [row._asdict() for row in users]


@app.get("/users/count")
def get_user_count(db: Session = Depends(get_db)):
    total = db.query(func.count(User.id)).scalar()
    return {"total_users": total}


@app.get("/users/by-location")
def get_users_by_location(location: str, db: Session = Depends(get_db)):
    users = (
        db.query(
            User.id,
            User.display_name,
            User.job_title,
            User.mail,
            User.mobile_phone,
            User.office_location,
        )
        .filter(User.office_location == location)
        .all()
    )
    return [row._asdict() for row in users]


@app.get("/users/by-mail")
def get_user_by_mail(mail: str, db: Session = Depends(get_db)):
    users = (
        db.query(
            User.id,
            User.display_name,
            User.job_title,
            User.mail,
            User.mobile_phone,
            User.office_location,
        )
        .filter(User.mail == mail)
        .all()
    )
    return [row._asdict() for row in users]


@app.get("/users/by-id")
def get_user_by_id(id: str, db: Session = Depends(get_db)):
    users = (
        db.query(
            User.id,
            User.display_name,
            User.job_title,
            User.mail,
            User.mobile_phone,
            User.office_location,
        )
        .filter(User.id == id)
        .all()
    )
    return [row._asdict() for row in users]


@app.get("/users/by-job-title")
def get_users_by_job_title(job_title: str, db: Session = Depends(get_db)):
    users = (
        db.query(
            User.id,
            User.display_name,
            User.job_title,
            User.mail,
            User.mobile_phone,
            User.office_location,
        )
        .filter(User.job_title == job_title)
        .all()
    )
    return [row._asdict() for row in users]


# DEVICES


@app.get("/devices/count")
def get_device_count(db: Session = Depends(get_db)):
    total = db.query(func.count(Device.id)).scalar()
    return {"total_devices": total}


@app.get("/devices/count/by-os")
def count_devices_by_os(operating_system: str, db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == operating_system)
        .scalar()
    )
    return {"operating_system": operating_system, "count": total}


@app.get("/devices/count/by-ownership")
def count_devices_by_ownership(ownership: str, db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.device_ownership == ownership)
        .scalar()
    )
    return {"ownership": ownership, "count": total}


@app.get("/devices/count/windows")
def count_windows_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "Windows")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/ios")
def count_ios_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(
            or_(Device.operating_system == "iOS", Device.operating_system == "IPhone")
        )
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/macos")
def count_macos_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "MacOS")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/android")
def count_android_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(
            or_(
                Device.operating_system == "Android",
                Device.operating_system == "AndroidForWork",
            )
        )
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/mac-mdm")
def count_macmdm_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "MacMDM")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/linux")
def count_linux_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "Linux")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/by-os")
def get_devices_by_os(operating_system: str, db: Session = Depends(get_db)):
    devices = (
        db.query(
            Device.id,
            Device.user_id,
            Device.device_category,
            Device.device_id,
            Device.device_ownership,
            Device.model,
            Device.display_name,
            Device.operating_system,
        )
        .filter(Device.operating_system == operating_system)
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/devices/by-ownership")
def get_devices_by_ownership(ownership: str, db: Session = Depends(get_db)):
    devices = db.query(Device).filter(Device.device_ownership == ownership).all()
    return devices


@app.get("/devices/by-user-id")
def get_devices_by_user_id(user_id: str, db: Session = Depends(get_db)):
    devices = (
        db.query(
            Device.id,
            Device.device_category,
            Device.device_id,
            Device.device_ownership,
            Device.device_version,
            Device.display_name,
            Device.domain_name,
        )
        .filter(Device.user_id == user_id)
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/devices/by-mail")
def get_devices_by_mail(mail: str, db: Session = Depends(get_db)):
    devices = (
        db.query(
            Device.id,
            Device.device_id,
            Device.display_name,
            Device.domain_name,
            Device.device_ownership,
            Device.operating_system,
        )
        .join(User, User.id == Device.user_id)
        .filter(User.mail == mail)
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/devices/by-azure-ad-id")
def get_device_by_azure_ad_id(azure_ad_device_id: str, db: Session = Depends(get_db)):
    devices = (
        db.query(
            ManagedDevice.id,
            ManagedDevice.user_id,
            ManagedDevice.device_name,
            ManagedDevice.managed_device_name,
            ManagedDevice.email_address,
            ManagedDevice.user_display_name,
            ManagedDevice.model,
        )
        .filter(ManagedDevice.azure_ad_device_id == azure_ad_device_id)
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/devices/by-wifi-mac")
def get_device_by_wifi_mac(wi_fi_mac_address: str, db: Session = Depends(get_db)):
    devices = (
        db.query(ManagedDevice)
        .filter(ManagedDevice.wi_fi_mac_address == wi_fi_mac_address)
        .all()
    )
    return devices


# MANAGED DEVICES

_MANAGED_DEVICE_FIELDS = (
    ManagedDevice.id,
    ManagedDevice.user_id,
    ManagedDevice.managed_device_name,
    ManagedDevice.azure_ad_device_id,
    ManagedDevice.email_address,
    ManagedDevice.user_display_name,
    ManagedDevice.model,
    ManagedDevice.manufacturer,
    ManagedDevice.wi_fi_mac_address,
    ManagedDevice.device_enrollment_type,
    ManagedDevice.operating_system,
)


@app.get("/managed-devices/count")
def count_all_managed_devices(db: Session = Depends(get_db)):
    total = db.query(func.count(ManagedDevice.id)).scalar()
    return {"count": total}


@app.get("/managed-devices/count/by-location")
def count_managed_devices_by_location(location: str, db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .join(User, User.id == ManagedDevice.user_id)
        .filter(User.office_location == location)
        .scalar()
    )
    return {"location": location, "count": total}


@app.get("/managed-devices/count/all-locations")
def count_managed_devices_all_locations(db: Session = Depends(get_db)):
    results = (
        db.query(User.office_location, func.count(ManagedDevice.id).label("count"))
        .join(ManagedDevice, ManagedDevice.user_id == User.id)
        .filter(User.office_location.isnot(None))
        .group_by(User.office_location)
        .all()
    )
    return [{"location": row.office_location, "count": row.count} for row in results]


@app.get("/managed-devices/count/unassigned")
def count_managed_devices_without_user(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.user_id.is_(None))
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/count/windows")
def count_managed_windows(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "Windows")
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/count/ios")
def count_managed_ios(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(
            or_(
                ManagedDevice.operating_system == "iOS",
                ManagedDevice.operating_system == "IPhone",
            )
        )
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/count/macos")
def count_managed_macos(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "macOS")
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/count/android")
def count_managed_android(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(
            or_(
                ManagedDevice.operating_system == "Android",
                ManagedDevice.operating_system == "AndroidForWork",
            )
        )
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/count/mac-mdm")
def count_managed_macmdm(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "MacMDM")
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/count/linux")
def count_managed_linux(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "Linux")
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/ios")
def get_managed_ios_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "iOS")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/managed-devices/android")
def get_managed_android_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "Android")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/managed-devices/macos")
def get_managed_macos_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "macOS")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/managed-devices/linux")
def get_managed_linux_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "Linux (ubuntu)")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/managed-devices/by-mail")
def get_managed_devices_by_mail(mail: str, db: Session = Depends(get_db)):
    devices = (
        db.query(
            ManagedDevice.id,
            ManagedDevice.device_name,
            ManagedDevice.operating_system,
            ManagedDevice.os_version,
            ManagedDevice.manufacturer,
            ManagedDevice.model,
            ManagedDevice.wi_fi_mac_address,
        )
        .join(User, User.id == ManagedDevice.user_id)
        .filter(User.mail == mail)
        .all()
    )
    return [row._asdict() for row in devices]
