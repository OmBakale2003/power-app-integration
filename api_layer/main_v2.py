from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, or_
from db.database import Database
from db.models import User, Device, ManagedDevice

app = FastAPI()

# Database instance
db_instance = Database("sqlite:///data.db")


# Dependency
def get_db():
    with db_instance.get_session() as session:
        yield session


# USER ENDPOINTS


@app.get("/user/count")
def get_user_count(db: Session = Depends(get_db)):
    total = db.query(func.count(User.id)).scalar()
    return {"total_users": total}


@app.get("/user/byLoc")
def get_user_by_location(location: str, db: Session = Depends(get_db)):
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


@app.get("/user/user_By_mail")
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


@app.get("/user/user_By_id")
def get_user_by_id(Id: str, db: Session = Depends(get_db)):
    users = (
        db.query(
            User.id,
            User.display_name,
            User.job_title,
            User.mail,
            User.mobile_phone,
            User.office_location,
        )
        .filter(User.id == Id)
        .all()
    )
    return [row._asdict() for row in users]


@app.get("/user/byJobTitle")
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


# DEVICE ENDPOINTS


@app.get("/device/count")
def get_device_count(db: Session = Depends(get_db)):
    total = db.query(func.count(Device.id)).scalar()
    return {"total_devices": total}


@app.get("/device/device_by_userid")
def get_device_by_user_id(UserId: str, db: Session = Depends(get_db)):
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
        .filter(Device.user_id == UserId)
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/device/Count_managed_Devices")
def count_managed_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.device_ownership == "Company")
        .scalar()
    )
    return {"company_managed_devices": total}


@app.get("/device/Get_all_ManagedDevices")
def get_all_managed_devices(device_ownerShip: str, db: Session = Depends(get_db)):
    devices = db.query(Device).filter(Device.device_ownership == device_ownerShip).all()
    return devices


@app.get("/device/get_using_wifi_Mac")
def get_device_by_mac(wi_fi_mac_address: str, db: Session = Depends(get_db)):
    devices = (
        db.query(ManagedDevice)
        .filter(ManagedDevice.wi_fi_mac_address == wi_fi_mac_address)
        .all()
    )
    return devices


@app.get("/device/byUserMail")
def get_devices_by_user_mail(mail: str, db: Session = Depends(get_db)):
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


@app.get("/device/getAllDeviceUsingMail")
def get_all_devices_by_mail(mail: str, db: Session = Depends(get_db)):
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


@app.get("/device/getUsingAd")
def get_device_by_ad(azure_ad_device_id: str, db: Session = Depends(get_db)):
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


# ── Device OS Counts ──────────────────────────────────────────────────────────


@app.get("/devices/count/Windows")
def count_windows_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "Windows")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/iphone")
def count_ios_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(
            or_(Device.operating_system == "iOS", Device.operating_system == "IPhone")
        )
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/MacOS")
def count_macos_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "MacOS")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/Android")
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


@app.get("/devices/count/MacMDM")
def count_macmdm_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "MacMDM")
        .scalar()
    )
    return {"count": total}


@app.get("/devices/count/Linux")
def count_linux_devices(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(Device.id))
        .filter(Device.operating_system == "Linux")
        .scalar()
    )
    return {"count": total}


# MANAGED DEVICE ENDPOINTS

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


@app.get("/ManagedDevices")
def count_all_managed_devices(db: Session = Depends(get_db)):
    total = db.query(func.count(ManagedDevice.id)).scalar()
    return {"count": total}


@app.get("/ManagedDevices/ios")
def get_managed_ios_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "iOS")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/ManagedDevices/android")
def get_managed_android_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "Android")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/ManagedDevices/macos")
def get_managed_macos_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "macOS")
        .all()
    )
    return [row._asdict() for row in devices]


@app.get("/ManagedDevices/linux")
def get_managed_linux_devices(db: Session = Depends(get_db)):
    devices = (
        db.query(*_MANAGED_DEVICE_FIELDS)
        .filter(ManagedDevice.operating_system == "Linux (ubuntu)")
        .all()
    )
    return [row._asdict() for row in devices]


# ── Managed Device OS Counts ──────────────────────────────────────────────────


@app.get("/ManagedDevices/count/Windows")
def count_managed_windows(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "Windows")
        .scalar()
    )
    return {"count": total}


@app.get("/ManagedDevices/count/iphone")
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


@app.get("/ManagedDevices/count/MacOS")
def count_managed_macos(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "MacOS")
        .scalar()
    )
    return {"count": total}


@app.get("/ManagedDevices/count/Android")
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


@app.get("/ManagedDevices/count/MacMDM")
def count_managed_macmdm(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "MacMDM")
        .scalar()
    )
    return {"count": total}


@app.get("/ManagedDevices/count/Linux")
def count_managed_linux(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "Linux")
        .scalar()
    )
    return {"count": total}


@app.get("/ManagedDevices/count/byLocation")
def count_managed_devices_by_location(location: str, db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .join(User, User.id == ManagedDevice.user_id)
        .filter(User.office_location == location)
        .scalar()
    )
    return {"location": location, "device_count": total}


@app.get("/ManagedDevices/count/allLocations")
def count_managed_devices_all_locations(db: Session = Depends(get_db)):
    results = (
        db.query(
            User.office_location, func.count(ManagedDevice.id).label("device_count")
        )
        .join(ManagedDevice, ManagedDevice.user_id == User.id)
        .filter(User.office_location.isnot(None))
        .group_by(User.office_location)
        .all()
    )
    return [
        {"location": row.office_location, "device_count": row.device_count}
        for row in results
    ]
