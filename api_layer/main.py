from fastapi import FastAPI
import sqlite3

app = FastAPI()
DB_name = "data.db"

def query_db(query, params=()):
    conn = sqlite3.connect(DB_name)
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return rows


@app.get("/user/count")
def get_all_users():
    result = query_db("select count(*) as total_users from users")
    return result

@app.get("/user/byLoc")
def get_user_byLoc(location: str):
    query = """
    SELECT u.id, u.display_name, u.job_title, u.mail, u.mobile_phone, u.office_location
    FROM users u
    WHERE u.office_location = ?
    """
    rows = query_db(query, (location,))
    return rows

@app.get("/user/user_By_mail")
def get_user_byMail(mail: str):
    query = """
    SELECT u.id, u.display_name, u.job_title, u.mail, u.mobile_phone, u.office_location
    FROM users u
    WHERE u.mail = ?
    """
    rows = query_db(query, (mail,))
    return rows

@app.get("/user/user_By_id")
def get_user_byId(Id: str):
    query = """
    SELECT u.id, u.display_name, u.job_title, u.mail, u.mobile_phone, u.office_location
    FROM users u
    WHERE u.id = ?
    """
    rows = query_db(query, (Id,))
    return rows

@app.get("/device/count")
def get_all_devices():
    result = query_db("select count(*) as total_devices from devices")
    return result


@app.get("/device/device_by_userid")
def get_Device_byUserId(UserId: str):
    query = """
    SELECT d.id,d.device_category,d.device_id,d.device_ownership,d.device_version,d.display_name,d.domain_name
    FROM devices d
    WHERE d.user_id = ?
    """
    rows = query_db(query, (UserId,))
    return rows

@app.get("/device/Count_managed_Devices")
def Count_ManagedDevices():
    query = """
    SELECT count(*) as company_managed_devices
    FROM devices
    WHERE device_ownership='Company'
    """
    rows = query_db(query)
    return rows

@app.get("/device/Get_all_ManagedDevices")
def get_ManagedDevices(device_ownerShip: str):
    query = """
    SELECT *
    FROM devices
    WHERE device_ownership = ?
    """
    rows = query_db(query, (device_ownerShip,))
    return rows

@app.get("/device/get_using_wifi_Mac")
def get_device_ByMac(wi_fi_mac_address: str):
    query = """
    SELECT *
    FROM managed_devices
    WHERE wi_fi_mac_address = ?
    """
    rows = query_db(query, (wi_fi_mac_address,))
    return rows

@app.get("/user/byJobTitle")
def get_users_by_job_title(job_title: str):
    query = """
    SELECT u.id, u.display_name, u.job_title, u.mail, u.mobile_phone, u.office_location
    FROM users u
    WHERE u.job_title = ?
    """
    rows = query_db(query, (job_title,))
    return rows

@app.get("/device/byUserMail")
def get_devices_by_user_mail(mail: str):
    query = """
    SELECT 
        m.id,
        m.device_name,
        m.operating_system,
        m.os_version,
        m.manufacturer,
        m.model,
        m.wi_fi_mac_address
    FROM managed_devices m
    JOIN users u ON u.id = m.user_id
    WHERE u.mail = ?
    """
    rows = query_db(query, (mail,))
    return rows

@app.get("/device/getAllDeviceUsingMail")
def get_devices_by_user_mail_all(mail: str):
    query = """
    SELECT 
        d.id,
        d.device_id,
        d.display_name,
        d.domain_name,
        d.device_ownership,
        d.operating_system
    FROM devices d
    JOIN users u ON u.id = d.user_id
    WHERE u.mail = ?
    """
    rows = query_db(query, (mail,))
    return rows

@app.get("/device/getUsingAd")
def getDeviceByAd(azure_ad_device_id: str):
    query = """
    SELECT 
        m.id,
        m.user_id,
        m.device_name,
        m.managed_device_name,
        m.email_address,
        m.user_display_name,
        m.model
    FROM managed_devices m
    WHERE m.azure_ad_device_id = ?
    """
    rows = query_db(query, (azure_ad_device_id,))
    return rows

@app.get("/devices/by-os")
def get_devices_by_os(operating_system: str):
    query = """
    SELECT 
        d.id,
        d.user_id,
        d.device_category,
        d.device_id,
        d.device_Ownership,
        d.model,
        d.display_name,
        d.device_ownership,
        d.operating_system
    FROM devices d
    WHERE d.operating_system = ?
    """
    rows = query_db(query, (operating_system,))
    return rows


@app.get("/devices/count/Windows")
def get_windows_devices():
    query = """
       select count(*) from devices where operating_System="Windows"

    """
    rows = query_db(query)
    return rows

@app.get("/devices/count/iphone")
def get_ios_devices():
    query = """
       select count(*) from devices where operating_System="iOS" or operating_System="IPhone"

    """
    rows = query_db(query)
    return rows

@app.get("/devices/count/MacOS")
def get_macos_devices():
    query = """
        select count(*) from devices where operating_System="MacOS"
    """
    rows = query_db(query)
    return rows

@app.get("/devices/count/Android")
def get_android_devices():
    query = """
        select count(*) from devices where operating_System="Android" or operating_System="AndroidForWork"

    """
    rows = query_db(query)
    return rows

@app.get("/devices/count/MacMDM")
def get_macmdm_devices():
    query = """
    select count(*) from devices where operating_System="MacMDM"
    """
    rows = query_db(query)
    return rows

@app.get("/devices/count/Linux")
def get_macmdm_devices():
    query = """
    select count(*) from devices where operating_System="Linux"
    """
    rows = query_db(query)
    return rows


@app.get("/ManagedDevices")
def get_all_managedDevices():
    query="""
    select count(*) from managed_devices"""
    rows=query_db(query)
    return rows

@app.get("/ManagedDevices/ios")
def get_ios_devices():
    query = """
    SELECT 
        d.id,
        d.user_id,
        d.managed_device_name,
        d.azure_ad_device_id,
        d.email_address,
        d.user_display_name,
        d.model,
        d.manufacturer,
        d.wi_fi_mac_address,
        d.device_enrollment_type,
        d.operating_system
    FROM managed_devices d
    WHERE d.operating_system = "iOS"
    """
    rows = query_db(query)
    return rows


@app.get("/ManagedDevices/android")
def get_ios_devices():
    query = """
    SELECT 
        d.id,
        d.user_id,
        d.managed_device_name,
        d.azure_ad_device_id,
        d.email_address,
        d.user_display_name,
        d.model,
        d.manufacturer,
        d.wi_fi_mac_address,
        d.device_enrollment_type,
        d.operating_system
    FROM managed_devices d
    WHERE d.operating_system = "Android"
    """
    rows = query_db(query)
    return rows


@app.get("/ManagedDevices/macos")
def get_ios_devices():
    query = """
    SELECT 
        d.id,
        d.user_id,
        d.managed_device_name,
        d.azure_ad_device_id,
        d.email_address,
        d.user_display_name,
        d.model,
        d.manufacturer,
        d.wi_fi_mac_address,
        d.device_enrollment_type,
        d.operating_system
    FROM managed_devices d
    WHERE d.operating_system = "macOS"
    """
    rows = query_db(query)
    return rows


@app.get("/ManagedDevices/linux")
def get_ios_devices():
    query = """
    SELECT 
        d.id,
        d.user_id,
        d.managed_device_name,
        d.azure_ad_device_id,
        d.email_address,
        d.user_display_name,
        d.model,
        d.manufacturer,
        d.wi_fi_mac_address,
        d.device_enrollment_type,
        d.operating_system
    FROM managed_devices d
    WHERE d.operating_system = "Linux (ubuntu)"
    """
    rows = query_db(query)
    return rows


@app.get("/ManagedDevices/count/Windows")
def get_windows_devices():
    query = """
       select count(*) from managed_devices where operating_System="Windows"

    """
    rows = query_db(query)
    return rows

@app.get("/ManagedDevices/count/iphone")
def get_ios_devices():
    query = """
       select count(*) from managed_devices where operating_System="iOS" or operating_System="IPhone"

    """
    rows = query_db(query)
    return rows

@app.get("/ManagedDevices/count/MacOS")
def get_macos_devices():
    query = """
        select count(*) from managed_devices where operating_System="MacOS"
    """
    rows = query_db(query)
    return rows

@app.get("/ManagedDevices/count/Android")
def get_android_devices():
    query = """
        select count(*) from managed_devices where operating_System="Android" or operating_System="AndroidForWork"

    """
    rows = query_db(query)
    return rows

@app.get("/ManagedDevices/count/MacMDM")
def get_macmdm_devices():
    query = """
    select count(*) from managed_devices where operating_System="MacMDM"
    """
    rows = query_db(query)
    return rows

@app.get("/ManagedDevices/count/Linux")
def get_macmdm_devices():
    query = """
    select count(*) from managed_devices where operating_System="Linux"
    """
    rows = query_db(query)
    return rows