import snowflake.connector
from snowflake.connector.errors import Error
import json

# Define connection parameters
conn_params = {
    'user': 'AWS_FS01_SYNC_USER',
    'password': '7RLB$os?RedCJE8H',
    'account': 'ek09803.ca-central-1.aws',
    'role': 'AWS_FS01_ROLE',
    'warehouse': 'COMPUTE_WH',
    'database': 'RPE_APOLLO',
    'schema': 'FLEETIO'
}

try:
    # Establish the connection
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**conn_params)
    print("Connection established.")
    
    # Create a cursor object
    cur = conn.cursor()
    
    # Verify connection by querying the current version
    cur.execute("SELECT current_version()")
    version = cur.fetchone()
    print(f"Connected to Snowflake, version: {version[0]}")
    
    # List all existing warehouses
    print("Listing existing warehouses...")
    cur.execute("SHOW WAREHOUSES")
    warehouses = cur.fetchall()
    if not warehouses:
        print("No warehouses found.")
    else:
        print("Existing warehouses:")
        for warehouse in warehouses:
            print(warehouse[0])
    
    # Check if the specified warehouse exists
    cur.execute("SHOW WAREHOUSES LIKE 'COMPUTE_WH'")
    warehouse_exists = cur.fetchone()
    if not warehouse_exists:
        raise ValueError("Warehouse 'COMPUTE_WH' does not exist or is not accessible.")
    
    # Use the specified warehouse
    cur.execute("USE WAREHOUSE COMPUTE_WH")
    
    # Check database existence
    cur.execute("SHOW DATABASES LIKE 'RPE_APOLLO'")
    if not cur.fetchone():
        raise ValueError("Database 'RPE_APOLLO' does not exist or is not accessible.")
    
    # Define your query (replace 'YOUR_TABLE' with your actual table name)
    query = "SELECT * FROM VEHICLES LIMIT 10;"
    
    # Execute the query
    cur.execute(query)
    
    # Fetch the results
    vehicle_data = cur.fetchall()
    
    # Get the field names
    field_names = [desc[0] for desc in cur.description]
    
    # Define the fields to include in the dictionary
    fields_to_include = {'ID', 'NAME', 'VEHICLE_STATUS_ID', 'TYPE_NAME', 'GROUP_NAME', 'DRIVER', 'SPECS', 'YEAR', 'CUSTOM_FIELDS'}
    specs_keys_to_include = {'body_subtype', 'transmission_type'}
    custom_keys_to_include = {'has_mod_containers'}
    driver_keys_to_include = {'id'}
    
    # Create a list of dictionaries with only the specified fields
    vehicles_list = []
    for vehicle in vehicle_data:
        vehicle_dict = {field: value for field, value in zip(field_names, vehicle) if field in fields_to_include}
        
        if 'SPECS' in vehicle_dict:
            specs_dict = json.loads(vehicle_dict['SPECS'])
            vehicle_dict['SPECS'] = {key: specs_dict.get(key) for key in specs_keys_to_include}
        
        if 'CUSTOM_FIELDS' in vehicle_dict:
            custom_fields_dict = json.loads(vehicle_dict['CUSTOM_FIELDS'])
            vehicle_dict['CUSTOM_FIELDS'] = {key: custom_fields_dict.get(key) for key in custom_keys_to_include}
        
        if 'DRIVER' in vehicle_dict:
            driver_fields_dict = json.loads(vehicle_dict['DRIVER'])
            vehicle_dict['DRIVER'] = {key: driver_fields_dict.get(key) for key in driver_keys_to_include}
        
        vehicles_list.append(vehicle_dict)
    
    # Print the list of dictionaries
    for vehicle_dict in vehicles_list:
        print(vehicle_dict)
        
except Error as e:
    print(f"An error occurred: {e}")
except ValueError as ve:
    print(ve)
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
