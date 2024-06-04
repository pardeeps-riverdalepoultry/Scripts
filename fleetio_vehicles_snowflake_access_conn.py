import snowflake.connector
from snowflake.connector.errors import Error
import json
import pyodbc
import getpass

def get_connection_params(user, password):
        
    return {
        'user': user,
        'password': password,
        'account': 'ek09803.ca-central-1.aws',
        'role': 'AWS_FS01_ROLE',
        'warehouse': 'COMPUTE_WH',
        'database': 'RPE_APOLLO',
        'schema': 'FLEETIO'
    }

def connect_to_snowflake(conn_params):
    try:
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(**conn_params)
        print("Connection established.")
        return conn
    except Error as e:
        print(f"An error occurred: {e}")
        return None

def retrieve_fleetio_vehicles_data(conn):
    try:
        cur = conn.cursor()
        
        # Verify connection by querying the current version
        cur.execute("SELECT current_version()")
        version = cur.fetchone()
        print(f"Connected to Snowflake, version: {version[0]}")
        
        # Use the specified warehouse
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        
        # Define your query (replace 'YOUR_TABLE' with your actual table name)
        query = "SELECT * FROM VEHICLES;"
        
        # Execute the query
        cur.execute(query)
        
        # Fetch the results
        vehicle_data = cur.fetchall()
        
        # Get the field names
        field_names = [desc[0] for desc in cur.description]
        
        # Create a list of tuples with only the specified fields
        vehicles_list = []
        for vehicle in vehicle_data:
            vehicle_dict = {field: value for field, value in zip(field_names, vehicle)}
            
            specs_dict = json.loads(vehicle_dict['SPECS']) if 'SPECS' in vehicle_dict else {}
            custom_fields_dict = json.loads(vehicle_dict['CUSTOM_FIELDS']) if 'CUSTOM_FIELDS' in vehicle_dict else {}
            driver_fields_dict = json.loads(vehicle_dict['DRIVER']) if 'DRIVER' in vehicle_dict else {}
            
            tuple_values = (
                vehicle_dict.get('ID'),
                vehicle_dict.get('NAME'),
                vehicle_dict.get('VEHICLE_STATUS_ID'),
                vehicle_dict.get('TYPE_NAME'),
                vehicle_dict.get('GROUP_NAME'),
                driver_fields_dict.get('id'),
                specs_dict.get('transmission_type'),
                specs_dict.get('body_subtype'),
                vehicle_dict.get('YEAR'),
                custom_fields_dict.get('has_mods_containers')
            )
            
            vehicles_list.append(tuple_values)
        
        # Print the list of tuples
        for vehicle_tuple in vehicles_list:
            print(vehicle_tuple)
        
    except Error as e:
        print(f"An error occurred: {e}")
    finally:
        # Ensure that the cursor and connection are closed
        try:
            cur.close()
        except NameError:
            pass
        
        try:
            conn.close()
        except NameError:
            pass

    return vehicles_list

# Function to refresh Fleetio Data in MS Access
def refresh_fleetio_vehicles(fleetio_data):
    access_db_path = r'G:\29.0 Data Infrastructure & Analytics\24. Automation\Processor Live Haul Schedules\houdini.accdb'
    connection_string = f'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={access_db_path}'

    try:
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # Delete all records in the table
        cursor.execute("DELETE FROM [fleetio_vehicle_data]")
        
        # Insert new records using executemany for bulk insert
        insert_query = """
        INSERT INTO [fleetio_vehicle_data] 
        (fleetio_id, vehicle_name, vehicle_status_id, type_name, group_name, assigned_operator_id, transmission_type, body_subtype, model_year, has_mods)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """
        
        cursor.executemany(insert_query, fleetio_data)

        connection.commit()
        print("Data updated successfully in Access database.")

    except pyodbc.Error as e:
        if connection:
            connection.rollback()
        print("Error updating Access database:", e)
    finally:
        if connection:
            connection.close()

def main():
    conn_params = get_connection_params('AWS_FS01_SYNC_USER', '7RLB$os?RedCJE8H')
    conn = connect_to_snowflake(conn_params)
    if conn:
        fleetio_data = retrieve_fleetio_vehicles_data(conn)
        refresh_fleetio_vehicles(fleetio_data)

if __name__ == "__main__":
    main()
