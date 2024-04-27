import requests

username = "api@riverdalepoultry.ca"
password = "5@?3QP#yd6dESfmY"
database = "riverdale"
server = "my792.geotab.com"

# Define the Geotab API endpoint
base_url = f"https://{server}.geotab.com/apiv1"

# Define the authentication endpoint
auth_endpoint = f"{base_url}/Authenticate"

# Create the authentication payload
auth_payload = {
    "userName": username,
    "password": password,
    "database": database
}

try:
    # Send the authentication request
    auth_response = requests.post(auth_endpoint, json=auth_payload)
    auth_response.raise_for_status()
    
    # Get the session token from the response
    session_token = auth_response.json()["credentials"]["sessionID"]
except requests.exceptions.RequestException as e:
    print(f"Authentication failed as: {e}")
    print("This is a test")
    print("This is test")
    print("New Features")
#this is a comment in new features
    exit(1)


# Replace 'your_device_id' with the actual ID or serial number of your device
device_id = "b15F"

# Define the device location endpoint
device_location_endpoint = f"{base_url}/Get"
device_location_payload = {
    "typeName": "DeviceStatusInfo",
    "search": {
        "id": device_id
    },
    "credentials": {
        "database": database,
        "sessionID": session_token
    },
    "resultsLimit": 5
}
#New comment
try:
    # Send the device location request
    location_response = requests.post(device_location_endpoint, json=device_location_payload)
    location_response.raise_for_status()
    
    # Parse the response to get latitude and longitude
    device_data = location_response.json()
    if device_data:
        latitude = device_data[0]["latitude"]
        longitude = device_data[0]["longitude"]
        print(f"Latitude: {latitude}, Longitude: {longitude}")
    else:
        print("Device not found.")
except requests.exceptions.RequestException as e:
    print(f"Error fetching device location: {e}")
finally:
    # Logout and close the session
    logout_endpoint = f"{base_url}/Logout"
    requests.post(logout_endpoint, json={"credentials": {"sessionID": session_token}})
