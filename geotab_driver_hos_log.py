# import necessary libraries
import mygeotab
from datetime import datetime, timedelta
import pytz
import dateutil.parser
import pyodbc

# Geotab credentials and server info
username = "api@riverdalepoultry.ca"
password = "5@?3QP#yd6dESfmY"
database = "riverdale"
server = "my792.geotab.com"
local_timezone = pytz.timezone("America/Toronto")

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


# Function to get logs info
def get_log():
    # Define yesterday's date
    yesterday_local = datetime.now() - timedelta(days=4) 
    yesterday_utc = yesterday_local.astimezone(pytz.utc)
    yesterday_utc_iso = yesterday_utc.isoformat() 
    search_params = {"fromDate": yesterday_utc_iso}  # Define search parameters
    log_records = []
    log_data = geotab_data("Get", "DutyStatusLog", search_params)  # Get log data from Geotab API

    # Process each log record
    for log in log_data:

        # Retrieving information
        log_id = log.get('id')
        device_status = log.get('status')
        log_origin = log.get('origin')
        log_state = log.get('state')
        log_malfunction = log.get('malfunction')
        log_version = log.get('version')
        log_sequence = log.get('sequence')
        log_eventRecordStatus = log.get('eventRecordStatus')
        log_eventCode = log.get('eventCode')
        log_eventType = log.get('eventType')
        log_deferralStatus = log.get('deferralStatus')
        log_deferralMinutes = log.get('deferralMinutes')
        log_isIgnored = log.get('isIgnored')
        log_eventCheckSum = log.get('eventCheckSum')
        log_isTransitioning = log.get('isTransitioning')

        # We need to use an if statement because the Driver ID and Device ID can be returned either as a dictionary or as a string
        if isinstance(log['driver'], dict):
            driver_id = log['driver']['id']
        else:
            driver_id = log['driver']

        if isinstance(log['device'], dict):
            device_id = log['device']['id']
        else:
            device_id = log['device']

        # Geotab returns log times in UTC time zone, need to convert it in GMT-4 time zone 
        log_datetime_utc = log['dateTime']
        log_datetime = str(log_datetime_utc)
        utc_datetime = dateutil.parser.parse(log_datetime)
        log_datetime_local = utc_datetime.astimezone(local_timezone)

        # changing the datetime format
        log_datetime_local = log_datetime_local.strftime("%Y-%m-%d %I:%M:%S %p")

        # creating a tuple of log records
        log_records.append((driver_id, log_id, log_datetime_local, log_datetime_utc, device_id, device_status,  log_origin, log_state, log_malfunction, log_version, log_sequence,  log_eventRecordStatus, log_eventCode, log_eventType, log_deferralStatus, log_deferralMinutes, log_isIgnored, log_eventCheckSum, log_isTransitioning))

    return log_records

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

# Function to refresh the user data table in MS Access
def refresh_user_data_table(user_records):
    access_db_path = r'G:\29.0 Data Infrastructure & Analytics\24. Automation\Processor Live Haul Schedules\houdini.accdb'
    connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_db_path}'

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Delete all records in the table
        cursor.execute("DELETE FROM [Geotab_Driver_Data]")
        
        # Process driver_records for updates, inserts, and deletions
        for record in user_records:
            user_id, user_fullname, user_fname, user_lname, user_guid, user_license, user_email, user_isDriver = record
            
            # Print the record being inserted for debugging
            print(f"Inserting record: {record}")

            sql = """
                INSERT INTO [Geotab_Driver_Data] ([driver_id], [Full_Name], [first_Name], [last_Name], [GUID], [license_Number], [email], [isDriver]) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = (user_id, user_fullname, user_fname, user_lname, user_guid, user_license, user_email, user_isDriver)
            
            # Print the SQL statement and values for debugging
            print(f"SQL: {sql}")
            print(f"Values: {values}")
            
            try:
                cursor.execute(sql, values)
            except pyodbc.Error as e:
                print(f"Error inserting record {record}: {e}")

        connection.commit()
        print("Data updated successfully in Access database.")

    except pyodbc.Error as e:
        print("Error updating Access database:", e)
    finally:
        if connection:
            connection.close()

# function to refresh driver logs in MS access
def refresh_driver_log(device_records):
    access_db_path = r'G:\29.0 Data Infrastructure & Analytics\24. Automation\Processor Live Haul Schedules\houdini.accdb'
    connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_db_path}'

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Delete all records in the table
        cursor.execute("DELETE FROM [hos_log_data]")
        
        # insert new records
        for driver_id, log_id, log_datetime_local, log_datetime_utc, device_id, device_status,  log_origin, log_state, log_malfunction, log_version, log_sequence,  log_eventRecordStatus, log_eventCode, log_eventType, log_deferralStatus, log_deferralMinutes, log_isIgnored, log_eventCheckSum, log_isTransitioning in device_records:
            cursor.execute("INSERT INTO [hos_log_data] (driver_id, log_id, log_datetime_local, log_datetime_utc, device_id, device_status,  log_origin, log_state, log_malfunction, log_version, log_sequence,  log_eventRecordStatus, log_eventCode, log_eventType, log_deferralStatus, log_deferralMinutes, log_isIgnored, log_eventCheckSum, log_isTransitioning) VALUES (?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                        (driver_id, log_id, log_datetime_local, log_datetime_utc, device_id, device_status,  log_origin, log_state, log_malfunction, log_version, log_sequence,  log_eventRecordStatus, log_eventCode, log_eventType, log_deferralStatus, log_deferralMinutes, log_isIgnored, log_eventCheckSum, log_isTransitioning))

        connection.commit()
        print("Data updated successfully in Access database.")

    except pyodbc.Error as e:
        if connection:
            connection.rollback
        print("Error updating Access database:", e)
    finally:
        if connection:
            connection.close()


# Entry point of the script
if __name__ == "__main__":
    # Call get_log function
    device_records = get_log()
    refresh_driver_log(device_records)
    user_records = get_user()
    refresh_user_data_table(user_records)