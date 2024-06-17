# Import libraries
import json
import requests
from datetime import datetime, timedelta

# Declare current time and start/end times to run code lines that gets all time off records
datetime_current = datetime.today().isoformat()
current_time = datetime.now().time()
# modulus number to control the frequency of the code lines that gets all time off records
current_hour = datetime.now().hour + 15
modulus_number = 24
modulus_calc = current_hour % modulus_number


# Namely API endpoints
namely_profile_url = r'''https://riverdalepoultry.namely.com/api/v1/profiles'''
namely_reports_url = r'''https://riverdalepoultry.namely.com/api/v1/reports/'''

# Namely report IDs
namely_report_ids = [{'profiles':''},
                     {'report_time_off_events':'c63f3579-c0a1-4716-9947-dbfeebe7eb08'},
                     {'report_unfiltered_time_off_events':'c63f3579-c0a1-4716-9947-dbfeebe7eb08'}]

# Function to get data from Namely API
def get_namely_data(api_root, headers):
    response = requests.get(api_root, headers=headers)
    response_data = json.loads(response.content.decode('utf-8'))
    return response_data

def lambda_handler(request, context):

    # Namely API token and headers
    secrets = request.get('secrets', {})
    namely_api_token = secrets.get('namely_api_token')
    request_headers = {'Authorization': 'Bearer ' + namely_api_token, 'Accept': 'application/json'}

    # *************************************************************
    # Get profile data from Namely API
    # *************************************************************
    profiles = get_namely_data(namely_profile_url, request_headers)
    profiles_data = []
    for profile in profiles['profiles']:
        profiles_data.append(profile)

    # *************************************************************
    # Get time off report data from Namely API
    # *************************************************************
    reports_url = namely_reports_url + namely_report_ids[1]['report_time_off_events']
    reports = get_namely_data(reports_url, request_headers)
    time_off_report_data = []
    all_time_off_report_data = []
    
    # Define the date threshold used to filter down the records for high frequency incremental updates in snowflake table
    date_threshold = datetime.now() - timedelta(days=10)
    
    for report in reports['reports']:
        columns = [column['label'] for column in report['columns']]
        
        # Identify the indices of 'Date Submitted' and 'GUID' columns
        date_submitted_index = columns.index('Date Submitted')
        date_updated_index = columns.index('Date Updated')
        leave_start_index = columns.index('Leave Start')
        guid_index = columns.index('GUID')
        
        for content in report['content']:
            # Extract values of 'Date Submitted' and 'GUID'
            date_submitted_value = str(content[date_submitted_index])
            date_updated_value = str(content[date_updated_index])
            leave_start_value = str(content[leave_start_index])
            guid_value = str(content[guid_index])
            
            # Parse the 'Date Submitted' value to a datetime object
            try:
                date_updated = datetime.strptime(date_updated_value, '%Y-%m-%d %I:%M %p')  # Adjust format if necessary
            except ValueError:
                continue  # Skip records with invalid date format

            # Check if the 'Date updated' is within the last 10 days
            if date_updated >= date_threshold:
                # Concatenate the required values
                concatenated_content = f"{guid_value} | {date_submitted_value} | {leave_start_value}"
                report_dict = {columns[i]: content[i] for i in range(len(columns))}
                # Create a new dictionary with 'record_id' as the first key
                new_report_dict = {'record_id': concatenated_content}
                new_report_dict.update(report_dict)
                time_off_report_data.append(new_report_dict)

            # Codelines that executes less frequently to update snowflake time_off_record_monitor table
            if modulus_calc == 0:
                # Concatenate the required values
                concatenated_content = f"{guid_value} | {date_submitted_value} | {leave_start_value}"
                report_dict = {columns[i]: content[i] for i in range(len(columns))}
                # Create a new dictionary with 'record_id' as the first key
                new_report_dict = {'record_id': concatenated_content}
                new_report_dict.update(report_dict)
                all_time_off_report_data.append(new_report_dict)

    # Construct JSON object with all data to send to Fivetran
    insert = {}
    # list of objects to be passed to list
    list = [profiles_data, time_off_report_data, all_time_off_report_data]
    
    # for loop to create insert object
    for i, report_id in enumerate(namely_report_ids):
        for key in report_id:
            insert[key] = list[i]

    response_to_fivetran = {
        'state': datetime_current,
        'insert': insert
    }
 
    return response_to_fivetran


if __name__ == '__main__':
    request = {'secrets': {'namely_api_token': 'ObSJcdRmuwUllCwRHgMZlDcn5briIxdgafxE2zfXTLgDDa0Am11kq1aRDVkl4yiq'}}
    lambda_handler(request, None)