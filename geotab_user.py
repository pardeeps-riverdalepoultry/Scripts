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

# Entry point of the script
if __name__ == "__main__":
    # Call get_log function
    user_records = get_user()
    refresh_user_data_table(user_records)


    