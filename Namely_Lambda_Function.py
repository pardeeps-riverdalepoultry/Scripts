# Import libraries
import json
import requests
import time
from datetime import datetime, timedelta
import csv

# Declare current time in ISO format used for fivetran object
datetime_current = datetime.today().isoformat()

# Namely API URL
namely_url = r'''https://riverdalepoultry.namely.com/api/v1/'''

# Configuration for the different Namely API endpoints and reports
config = {
    'profile': {
        'aws_s3_folder_name': 'profiles',
        'guid': '',
        'hour_modulus': 8,
        'endpoint': 'profiles',
        'date_threshold': None,
        'returned_namely_data_key': 'profiles'
    },
    'time_off_event': {
        'aws_s3_folder_name': 'report_time_off_events',
        'guid': '/b08612ec-fc77-4224-9425-0362444e120f',
        'hour_modulus': 1,
        'endpoint': 'reports',
        'date_threshold': 10,
        'returned_namely_data_key': 'reports'
    },
    'all_time_off_events': {
        'aws_s3_folder_name': 'report_unfiltered_time_off_events',
        'guid': '/b08612ec-fc77-4224-9425-0362444e120f',
        'hour_modulus': 24,
        'endpoint': 'reports',
        'date_threshold': None,
        'returned_namely_data_key': 'reports'
    },
    'group': {
        'aws_s3_folder_name': 'groups',
        'guid': '',
        'hour_modulus': 24,
        'endpoint': 'groups',
        'date_threshold': None,
        'returned_namely_data_key': 'groups'
    }
}

# Function to get paginated data from Namely API
def get_namely_data(api_root, headers, endpoint,namely_returned_data_key , per_page=50):
    page = 1  # Start page
    all_data = []

    # Check if the endpoint is 'profiles' to handle pagination
    if endpoint == 'profiles':
        while page <= 8:  # last page
            url = f"{api_root}.json?page={page}&per_page={per_page}&sort=-updated_at"
            response = requests.get(url, headers=headers)
            response_data = response.json()
            if 'profiles' in response_data:
                all_data.extend(response_data['profiles'])
                if len(response_data['profiles']) < per_page:
                    break
            else:
                break
            page += 1
    # Get data from the reports endpoint
    else:
        response = requests.get(api_root, headers=headers)
        response_data = json.loads(response.content.decode('utf-8'))
        all_data.extend(response_data[namely_returned_data_key])

    return all_data


# Function to process report data
def process_report_data(content, columns, guid_value, date_submitted_value, leave_start_value, target_list):
    concatenated_content = f"{guid_value} | {date_submitted_value} | {leave_start_value}"
    report_dict = {columns[i]: content[i] for i in range(len(columns))}
    report_dict['record_id'] = concatenated_content
    return report_dict

# Lambda handler function
def lambda_handler(request, context):

    # Namely API token and headers
    secrets = request.get('secrets', {})
    namely_api_token = secrets.get('namely_api_token')
    request_headers = {'Authorization': 'Bearer ' + namely_api_token, 'Accept': 'application/json'}
    insert = {}

    for key, value in config.items():
        list_name = value['aws_s3_folder_name']
        insert[list_name] = []
        endpoint = value['endpoint']
        url = namely_url + endpoint
        guid = value['guid']
        hour_modulus = value['hour_modulus']
        namely_returned_data_key = value['returned_namely_data_key']
        # if date_threshold is not set, set it to 0, this is because or returns the first truthy value. None is considered as False
        date_filter = value['date_threshold'] or 0

        # controls the frequency of the data pull
        modulus_calc = (datetime.now().hour) % hour_modulus
        if modulus_calc == 0:
            # Get data from Namely API
            data = get_namely_data(url + guid, request_headers, endpoint,namely_returned_data_key)
            # Process data based on the key
            if key == 'profile':
                for profile in data:
                    insert[list_name].append(profile)

            if key == 'group':
                for group in data:
                    insert[list_name].append(group)
                
            elif key == 'time_off_event' or key == 'all_time_off_events':
                for report in data:
                    columns = [column['label'] for column in report['columns']]
                    date_submitted_index = columns.index('Date Submitted')
                    date_updated_index = columns.index('Date Updated')
                    leave_start_index = columns.index('Leave Start')
                    guid_index = columns.index('GUID')
                    date_threshold = datetime.now() - timedelta(days=date_filter)
                    for content in report['content']:
                        date_submitted_value = str(content[date_submitted_index])
                        date_updated_value = str(content[date_updated_index])
                        leave_start_value = str(content[leave_start_index])
                        guid_value = str(content[guid_index])

                        # Parse the 'Date Submitted' value to a datetime object
                        try:
                            date_updated = datetime.strptime(date_updated_value, '%Y-%m-%d %I:%M %p')  # Adjust format if necessary
                        except ValueError:
                            continue  # Skip records with invalid date format

                        # Check if the 'Date Updated' is within the last 10 days and the key is 'time_off_event'
                        if date_updated >= date_threshold and key == 'time_off_event':
                            report_data = process_report_data(content, columns, guid_value, date_submitted_value, leave_start_value, list_name)
                            insert[list_name].append(report_data)

                        # Execute less frequently but pulls all data from time off report
                        if key == 'all_time_off_events':
                            report_data = process_report_data(content, columns, guid_value, date_submitted_value, leave_start_value, list_name)
                            insert[list_name].append(report_data)
    
    response_to_fivetran = {
        'state': datetime_current,
        'insert': insert
    }
 
    return response_to_fivetran


if __name__ == '__main__':
    script_start_time = time.time()
    request = {'secrets': {'namely_api_token': 'ObSJcdRmuwUllCwRHgMZlDcn5briIxdgafxE2zfXTLgDDa0Am11kq1aRDVkl4yiq'}}
    lambda_handler(request, None)
    script_end_time = time.time()
    print(f"Time taken to run the script: {script_end_time - script_start_time} seconds")
