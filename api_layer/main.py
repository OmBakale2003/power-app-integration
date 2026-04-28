import os
import pprint
from collections import defaultdict
from contextlib import asynccontextmanager
import logging
from config import DATABASE_URL
from auth import get_api_key

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from db.database import Database
from db.models import User, Device, ManagedDevice
from utils.data_transform_utils import group_office_location_to_flat_table

# Import our new scheduler logic
from scheduler.scheduler import setup_scheduler

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up FastAPI and Background Scheduler...")

    # Initialize and start the scheduler
    scheduler = setup_scheduler()
    scheduler.start()

    yield  # Application is now running and serving requests

    # Shutdown gracefully
    logger.info("Shutting down FastAPI and Background Scheduler...")
    scheduler.shutdown(wait=False)


# Attach lifespan to FastAPI
app = FastAPI(lifespan=lifespan, dependencies=[Depends(get_api_key)])

# Database setup for the API
db_instance = Database(DATABASE_URL)


def get_db():
    with db_instance.get_session() as session:
        yield session


# -- default path response
@app.get("/")
def home_path():
    return "server is up and running."


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


@app.get("/managed-devices/by-wifi-mac")
def get_device_by_wifi_mac(wi_fi_mac_address: str, db: Session = Depends(get_db)):
    devices = (
        db.query(ManagedDevice)
        .filter(ManagedDevice.wi_fi_mac_address == wi_fi_mac_address)
        .all()
    )
    return devices


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


@app.get("/managed-devices/count/linux")
def count_managed_linux(db: Session = Depends(get_db)):
    total = (
        db.query(func.count(ManagedDevice.id))
        .filter(ManagedDevice.operating_system == "Linux (ubuntu)")
        .scalar()
    )
    return {"count": total}


@app.get("/managed-devices/by-azure-ad-id")
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


# specific endpoints for power bi.
@app.get("/power-bi/api-1")
def get_count_of_devices_groupedby_user_job_titles_and_location(
    db: Session = Depends(get_db),
):
    try:
        result = (
            db.query(
                func.count(ManagedDevice.id).label("device_count"),
                User.job_title,
                User.office_location,
            )
            .join(User, User.id == ManagedDevice.user_id)
            .filter(User.office_location.is_not(None))
            .group_by(User.office_location, User.job_title)
            .all()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return [
        {
            "device_count": row.device_count,
            "job_title": row.job_title,
            "office_location": row.office_location,
        }
        for row in result
    ]


# Paginated APIs
@app.get("/users")
def user_paginated_api(
    _page: int | None = None,
    _select: str | None = None,
    _skip: int | None = None,
    _limit: int = 10,
    db: Session = Depends(get_db),
):
    selected_fields = [f.strip() for f in _select.split(",")] if _select else None

    if selected_fields:
        column_attributes = []
        for field in selected_fields:
            attr = getattr(User, field, None)
            if attr is None:
                raise HTTPException(status_code=400, detail=f"Invalid field: '{field}'")
            column_attributes.append(attr)
        query = db.query(*column_attributes)
    else:
        query = db.query(User)

    total = query.count()
    full_data = query.all()

    if _page is None and _skip is None:
        return {
            "data": [dict(row._mapping) for row in full_data]
            if selected_fields
            else full_data
        }

    if _page is not None:
        if _skip is not None:
            raise HTTPException(
                status_code=400, detail="Use either _page or _skip, not both"
            )
        _skip = (_page - 1) * _limit if _limit else 0

    if _skip is not None:
        query = query.offset(_skip)

    query = query.limit(_limit)
    paginated_data = query.all()

    serialized = (
        [dict(row._mapping) for row in paginated_data]
        if selected_fields
        else paginated_data
    )

    next_idx = (_skip or 0) + len(paginated_data)
    can_continue = next_idx < total

    return {
        "data": serialized,
        "pagination": {
            "total_count": total,
            "current_count": len(paginated_data),
            "can_continue": can_continue,
            "current_page": _page,
            "current_skip": _skip,
            "next_page": _page + 1 if _page and can_continue else None,
            "next_skip": next_idx if can_continue else None,
            "limit": _limit,
        },
    }


@app.get("/devices")
def devices_paginated_api(
    _page: int | None = None,
    _select: str | None = None,
    _skip: int | None = None,
    _limit: int = 10,
    db: Session = Depends(get_db),
):
    selected_fields = [f.strip() for f in _select.split(",")] if _select else None

    if selected_fields:
        column_attributes = []
        for field in selected_fields:
            attr = getattr(Device, field, None)
            if attr is None:
                raise HTTPException(status_code=400, detail=f"Invalid field: '{field}'")
            column_attributes.append(attr)
        query = db.query(*column_attributes)
    else:
        query = db.query(Device)

    full_data = query.all()

    if _page is None and _skip is None:
        return {
            "data": [dict(row._mapping) for row in full_data]
            if selected_fields
            else full_data
        }

    total = query.count()

    if _page is not None:
        if _skip is not None:
            raise HTTPException(
                status_code=400, detail="Use either _page or _skip, not both"
            )
        _skip = (_page - 1) * _limit

    if _skip is not None:
        query = query.offset(_skip)

    query = query.limit(_limit)

    paginated_data = query.all()

    serialized = (
        [dict(row._mapping) for row in paginated_data]
        if selected_fields
        else paginated_data
    )

    next_idx = (_skip or 0) + len(paginated_data)
    can_continue = next_idx < total

    return {
        "data": serialized,
        "pagination": {
            "total_count": total,
            "current_count": len(paginated_data),
            "can_continue": can_continue,
            "current_page": _page,
            "current_skip": _skip,
            "next_page": _page + 1 if _page and can_continue else None,
            "next_skip": next_idx if can_continue else None,
            "limit": _limit,
        },
    }


@app.get("/managed_devices")
def managed_devices_paginated_api(
    _page: int | None = None,
    _select: str | None = None,
    _skip: int | None = None,
    _limit: int = 10,
    db: Session = Depends(get_db),
):
    selected_fields = [f.strip() for f in _select.split(",")] if _select else None

    if selected_fields:
        column_attributes = []
        for field in selected_fields:
            attr = getattr(ManagedDevice, field, None)
            if attr is None:
                raise HTTPException(status_code=400, detail=f"Invalid field: '{field}'")
            column_attributes.append(attr)
        query = db.query(*column_attributes)
    else:
        query = db.query(ManagedDevice)

    total = query.count()

    full_data = query.all()

    if _page is None and _skip is None:
        return {
            "data": [dict(row._mapping) for row in full_data]
            if selected_fields
            else full_data
        }

    if _page is not None:
        if _skip is not None:
            raise HTTPException(
                status_code=400, detail="Use either _page or _skip, not both"
            )
        _skip = (_page - 1) * _limit

    if _skip is not None:
        query = query.offset(_skip)

    query = query.limit(_limit)

    paginated_data = query.all()

    serialized = (
        [dict(row._mapping) for row in paginated_data]
        if selected_fields
        else paginated_data
    )

    next_idx = (_skip or 0) + len(paginated_data)
    can_continue = next_idx < total

    return {
        "data": serialized,
        "pagination": {
            "total_count": total,
            "current_count": len(paginated_data),
            "can_continue": can_continue,
            "current_page": _page,
            "current_skip": _skip,
            "next_page": _page + 1 if _page and can_continue else None,
            "next_skip": next_idx if can_continue else None,
            "limit": _limit,
        },
    }


# API for cleaned User.office_location
@app.get("/users/office_location/grouped-by-region-and-city")
def users_office_location_grouped(db: Session = Depends(get_db)):
    data = db.query(User.office_location).all()

    office_locations = list(
        {
            row._mapping.get("office_location")
            for row in data
            if row._mapping.get("office_location") is not None
        }
    )

    data_to_return = group_office_location_to_flat_table(office_locations)

    """ freq = defaultdict(int)

    for row in data_to_return:
        freq[row["original_value"]] += 1

    freq = dict(sorted(freq.items(), key=lambda item: item[0]))

    pprint.pp(freq) """

    return {"data": data_to_return}
