# import necessary libraries
import mygeotab
import pyodbc


# Geotab credentials and server info
username = "api@riverdalepoultry.ca"
password = "5@?3QP#yd6dESfmY"
database = "riverdale"
server = "my792.geotab.com"


# Geotab Call
def geotab_data(call_type, typename, search_params=None):
    # authenticate with Geotab API
    api = mygeotab.API(username=username, password=password, database=database)
    try:
        api.authenticate()
        # perform Geotab API call
        if search_params:
            device_info = api.call(call_type, typeName=typename, search=search_params)
        else:
            device_info = api.call(call_type, typeName=typename)
    except mygeotab.exceptions.AuthenticationException:
        print("Failed to authenticate to Geotab")
    except Exception as e:
        print("An error occurred:", e)
    return device_info


# Function to get driver info
def get_user():
    # Define yesterday's date
    users_data = geotab_data("Get", "User")  # Get log data from Geotab API
    user_record = []

    # Process each log record
    for user in users_data:

        user_id = user.get('id')
        user_fname = user.get('firstName')
        user_lname = user.get('lastName')
        user_fullname = user_fname + " " + user_lname
        user_guid = user.get('employeeNo')
        user_email = user.get('name')
        user_license = user.get('licenseNumber')
        user_isDriver = user.get('isDriver')


        # creating a tuple of log records
        user_record.append((user_id, user_fullname, user_fname, user_lname, user_guid, user_license, user_email, user_isDriver))

    return user_record

def upsert_access_database(device_records):
    access_db_path = r'G:\29.0 Data Infrastructure & Analytics\24. Automation\Processor Live Haul Schedules\houdini.accdb'
    connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_db_path}'

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Delete all records in the table
        cursor.execute("DELETE FROM [Geotab_Driver_Data]")
        
        # Process driver_records for updates, inserts, and deletions
        for user_id, user_fullname, user_fname, user_lname, user_guid, user_license, user_email, user_isDriver in device_records:
            cursor.execute("INSERT INTO [Geotab_Driver_Data] (user_id, user_fullname, user_fname, user_lname, user_guid, user_license, user_email, user_isDriver) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                        (user_id, user_fullname, user_fname, user_lname, user_guid, user_license, user_email, user_isDriver))

        connection.commit()
        print("Data updated successfully in Access database.")

    except pyodbc.Error as e:
        print("Error updating Access database:", e)
    finally:
        if connection:
            connection.close()


# Entry point of the script
if __name__ == "__main__":
    # Call get_log function
    device_records = get_user()
    upsert_access_database(device_records)


    