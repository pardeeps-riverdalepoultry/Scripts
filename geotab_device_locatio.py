import mygeotab
import pyodbc

# Your Geotab credentials and server info
username = "api@riverdalepoultry.ca"
password = "5@?3QP#yd6dESfmY"
database = "riverdale"
server = "my792.geotab.com"

#Geotab Call
def geotab_data(call_type, typename):

    api = mygeotab.API(username=username, password=password, database=database)
    try:
        api.authenticate()
        device_info = api.call(call_type, typeName=typename)
    except mygeotab.exceptions.AuthenticationException:
        print("Failed to authenticate to Geotab")
    except Exception as e:
        print("An error occurred:", e)
    return device_info


#function to get device location
def get_location():

    device_locations_list = []

    location_data = geotab_data("Get", "DeviceStatusInfo")
    for device in location_data:
        device_id = device["device"]["id"]
        device_latitude = device["latitude"]
        device_longitude = device["longitude"]
        device_communicating = device["isDeviceCommunicating"]
        device_speed = device["speed"]
        device_locations_list.append([device_id, device_latitude, device_longitude, device_communicating, device_speed])

    return device_locations_list

#function to get device info:
def get_device_info():

    device_info = []

    device_data = geotab_data("Get", "Device")
    for device in device_data:
        device_id = device["id"]
        device_name = device["name"]
        device_info.append([device_id, device_name])

    return device_info

def upsert_access_database(device_records):
    access_db_path = r'G:\29.0 Data Infrastructure & Analytics\24. Automation\Processor Live Haul Schedules\houdini.accdb'
    connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_db_path}'

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Get a list of all device_id values currently in the table
        cursor.execute("SELECT device_id FROM [geotab_device_coordinates]")
        existing_device_ids = set(row.device_id for row in cursor.fetchall())

        # Process device_records for updates, inserts, and deletions
        for device_id, latitude, longitude, is_communicating, speed, device_name in device_records:
            # Check if the device_id already exists in the table
            if device_id in existing_device_ids:
                # Update latitude and longitude for existing device_id
                cursor.execute("UPDATE [geotab_device_coordinates] SET latitude=?, longitude=?, is_communicating=?, speed=?, device_name=? WHERE device_id=?", (latitude, longitude, is_communicating, speed, device_name, device_id))
                existing_device_ids.remove(device_id)
            else:
                # Insert a new record for device_id not in the table
                cursor.execute("INSERT INTO [geotab_device_coordinates] (device_id, latitude, longitude, is_communicating, speed, device_name) VALUES (?, ?, ?, ?, ?, ?)", (device_id, latitude, longitude, is_communicating, speed, device_name))

        # Delete records for device_ids that are in the table but not in device_records
        for device_id_to_delete in existing_device_ids:
            cursor.execute("DELETE FROM [geotab_device_coordinates] WHERE device_id=?", (device_id_to_delete,))

        connection.commit()
        print("Data updated successfully in Access database.")

    except pyodbc.Error as e:
        print("Error updating Access database:", e)
    finally:
        if connection:
            connection.close()



if __name__ == "__main__":
    device_records = get_device_info()
    device_location_list = get_location()
    device_id_to_name = {item[0]: item[1] for item in device_records}

    for item in device_location_list:
        device_id = item[0]
        if device_id in device_id_to_name:
            item.append(device_id_to_name[device_id])

    upsert_access_database(device_location_list)