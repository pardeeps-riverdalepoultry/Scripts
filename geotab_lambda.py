import json
import mygeotab
import logging
from datetime import datetime, timedelta, time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Time difference in hours for data logs
timedifference = 4
rule_ids = [{"id": "aJt467ee7rE288rop0Bb84w"}, {"id": "RulePostedSpeedingId"},
            {"id": "aJq0qW6S5z0WgwuUCyaxPzQ"}, {"id": "RuleHarshBrakingId"}, {"id": "aNcWNEox5_UuG-9L9DszYuA"},
            {"id": "RuleHarshCorneringId"}, {"id": "RuleSeatbeltId"}, {"id": "RuleJackrabbitStartsId"}]

def convert_datetime_to_string(data):
    """
    Recursively converts all datetime and time objects in the data to their string representations.
    
    Args:
    - data: The data to process
    
    Returns:
    - The processed data with datetime and time objects converted to strings
    """
    if isinstance(data, list):
        return [convert_datetime_to_string(item) for item in data]
    elif isinstance(data, dict):
        converted_data = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                converted_data[key] = value.isoformat()
            elif isinstance(value, time):
                converted_data[key] = value.isoformat()
            else:
                converted_data[key] = convert_datetime_to_string(value)
        return converted_data
    else:
        return data

def get_geotab_data(api, api_endpoint, fromDate, toDate, search_params=None):
    fromDate = fromDate.split('.')[0]
    toDate = toDate.split('.')[0]

    fromDate = datetime.strptime(fromDate, '%Y-%m-%dT%H:%M:%S')
    toDate = datetime.strptime(toDate, '%Y-%m-%dT%H:%M:%S')

    fromDate -= timedelta(hours=timedifference)

    logger.info(f'Fetching data from {fromDate} to {toDate} for {api_endpoint}')
    
    data = api.get(api_endpoint, resultsLimit=50000, search=search_params or {})
    data = convert_datetime_to_string(data)

    # Filter out results with UnknownDriverId
    filtered_data = []
    for trip in data:
        driver = trip.get('driver')
        if isinstance(driver, dict) and driver.get('id') != 'UnknownDriverId':
            filtered_data.append(trip)
    
    return filtered_data

def get_exception_event_data(api, fromDate, rule_ids):
    combined_results = []
    for rule_id in rule_ids:
        search_params = {
            "fromDate": fromDate,
            "ruleSearch": rule_id
        }
        exception_data = api.call('Get', typeName='ExceptionEvent', search=search_params)
        
        # Filter out results with UnknownDriverId
        filtered_data = []
        for event in exception_data:
            driver = event.get('driver')
            if isinstance(driver, dict) and driver.get('id') != 'UnknownDriverId':
                filtered_data.append(event)
        
        combined_results.extend(filtered_data)
    combined_results = convert_datetime_to_string(combined_results)
    return combined_results

def get_duty_status_violation_data(api, user_id, from_date, to_date):
    search_params = {
        "fromDate": from_date,
        "toDate": to_date,
        "userSearch": {
            "id": user_id
        }
    }
    data = api.get("DutyStatusViolation", search=search_params)
    return data

def get_user_and_device_data(api, from_date, to_date):
    user_device_data = {}

    # Define search parameters
    search_params = {
        "fromDate": from_date,
        "toDate": to_date
    }

    # Get updated Device records
    logger.info(f'Fetching Device data from {from_date} to {to_date}')
    user_device_data['Device'] = api.get('Device', resultsLimit=50000, search=search_params)

    # Get updated User records
    logger.info(f'Fetching User data from {from_date} to {to_date}')
    user_device_data['User'] = api.get('User', resultsLimit=50000, search=search_params)

    user_device_data = convert_datetime_to_string(user_device_data)
    
    return user_device_data

def should_run_device_and_user(current_time):
    start_time = current_time.replace(hour=14, minute=0, second=0, microsecond=0)
    end_time = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
    return start_time <= current_time < end_time


def DeviceStatusInfo(api):
    location_data = {}
    
    location_data['Location'] = api.get('DeviceStatusInfo', resultsLimit=50000)
    location_data = convert_datetime_to_string(location_data)
    return location_data

def lambda_handler(request, context):
    # Extract secrets from the request payload
    secrets = request.get('secrets', {})
    username = secrets.get('username')
    password = secrets.get('password')
    database = secrets.get('database')

    if not username or not password or not database:
        logger.error("Missing API credentials in the secrets")
        return {
            'statusCode': 400,
            'body': json.dumps('Missing API credentials in the secrets')
        }

    current_datetime = datetime.now()
    fromDate = (current_datetime - timedelta(hours=timedifference)).isoformat()
    toDate = current_datetime.isoformat()
    
    # Define separate fromDate and toDate for User and Device endpoints (last 36 hours)
    user_device_fromDate = (current_datetime - timedelta(hours=36)).isoformat()
    user_device_toDate = toDate

    api = mygeotab.API(username=username, password=password, database=database)
    api.authenticate()

    #################
    # adding locations data to insert object
    ##################

    # Initializing the dictionaries
    insert = {}
    
    # Fetching device status info and renaming the key
    device_status_info = DeviceStatusInfo(api)
    device_status_info['DeviceStatusInfo'] = device_status_info.pop('Location')

    # Updating the insert dictionary
    insert['DeviceStatusInfo'] = device_status_info['DeviceStatusInfo']
    # save device_status_info['DeviceStatusInfo'] as json file
    with open('device_status_info.json', 'w') as f:
        json.dump(device_status_info, f)


    #################

    dvirlog_params = {'fromDate': fromDate, 'toDate': toDate}
    insert['DVIRLog'] = get_geotab_data(api, 'DVIRLog', fromDate, toDate, dvirlog_params)

    insert['ExceptionEvent'] = get_exception_event_data(api, fromDate, rule_ids)

    # Get user IDs directly
    users = api.get("User")
    user_ids = [user["id"] for user in users]
    
    duty_status_violation_data = []
    for user_id in user_ids:
        duty_status_violation_data.extend(get_duty_status_violation_data(api, user_id, fromDate, toDate))
    insert['DutyStatusViolation'] = convert_datetime_to_string(duty_status_violation_data)
    trip_params = {'fromDate': fromDate, 'toDate': toDate}
    insert['Trip'] = get_geotab_data(api, 'Trip', fromDate, toDate, trip_params)

    if should_run_device_and_user(current_datetime):
        logger.info(f'Current time {current_datetime} is between 14:00 and 16:00 UTC, running Device and User endpoints.')
        user_device_data = get_user_and_device_data(api, user_device_fromDate, user_device_toDate)
        insert.update(user_device_data)
    else:
        logger.info(f'Skipped as current time {current_datetime} is not between 14:00 and 16:00 UTC')

    response_to_fivetran = {
        'state': toDate,
        'insert': insert
    }

    response_to_fivetran = convert_datetime_to_string(response_to_fivetran)

    return response_to_fivetran


if __name__ == '__main__':
    username = "api@riverdalepoultry.ca"
    password = "5@?3QP#yd6dESfmY"
    database = "riverdale"
    server = "my792.geotab.com"
    request = {
        "secrets": {
            "username": username,
            "password": password,
            "database": database,
            "server": server
        }
    }
    lambda_handler(request, None)
