import snowflake.connector
from snowflake.connector.errors import Error
import os
import pickle
from datetime import date, datetime
import json
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64

SCOPES = [
    'https://www.googleapis.com/auth/admin.directory.user',  # Admin SDK scope
    'https://www.googleapis.com/auth/contacts',  # Google Contacts scope
    'https://www.googleapis.com/auth/gmail.send' # Gmail scope
]
# Google contacts labels
std_label_id = '52893d4a0d6d4643'
exec_label_id = '639c56d38f4616ed'
# google_field_update_list = 'names,memberships,emailAddresses,phoneNumbers,relations,userDefined,organizations,birthdays,addresses'
google_field_update_list = 'names,emailAddresses,organizations,phoneNumbers,memberships,userDefined,birthdays,addresses,relations'

def get_connection_params(user, password):
        
    return {
        'user': user,
        'password': password,
        'account': 'ek09803.ca-central-1.aws',
        'role': 'AWS_FS01_ROLE',
        'warehouse': 'COMPUTE_WH',
        'database': 'RPE_APOLLO',
        'schema': 'NAMELY'
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

def retrieve_profile_data(conn, sql_query):
    profile_data = []
    profile_field_names = []
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute(sql_query)
        profile_data = cur.fetchall()
        profile_field_names = [field[0] for field in cur.description]
        
    except Error as e:
        print(f"An error occurred: {e}")
    finally:
        try:
            cur.close()
        except NameError:
            pass
        try:
            conn.close()
        except NameError:
            pass
    return profile_data, profile_field_names

def convert_to_dict(profile_data, profile_field_names):

    """
    Converts profile data to a list of dictionaries with field names as keys and parses 'Family_info' from a JSON string to a list.
    Args: profile_data (list of tuples), profile_field_names (list of str). Returns: list of dict.
    """
    dict_data = []
    for record in profile_data:
        record_dict = dict(zip(profile_field_names, record))
        
        # Check if 'Family_info' field exists and convert it from JSON string to Python list
        if 'FAMILY_INFO' in record_dict and isinstance(record_dict['FAMILY_INFO'], str):
            try:
                # Parse the JSON string to a Python object (list of dictionaries)
                record_dict['FAMILY_INFO'] = json.loads(record_dict['FAMILY_INFO'])
            except json.JSONDecodeError:
                # Handle JSON decoding errors if necessary
                print(f"Error decoding JSON for FAMILY_INFO: {record_dict['FAMILY_INFO']}")

        dict_data.append(record_dict)
    
    return dict_data

def authenticate_google():
    # Authenticate with Google APIs
    print('Authenticating with Google API')
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds

def search_dict_list(search_key, search_value, dic_list):
    # Retrieves items from a list of dictionaries based on a desired key and value to search for in the dictionaries
    return [element for element in dic_list if element[search_key] == search_value]

def get_google_contact():
    creds = authenticate_google()
    contacts_service = build('people', 'v1', credentials=creds)

    keep_iterating = True
    goog_contacts = []
    next_page_token = None
    resource = 'people/me'
    page_size = 1000
    fields = 'names,userDefined'

    while keep_iterating:
        if next_page_token is None:
            results = contacts_service.people().connections().list(
                resourceName=resource,
                pageSize=page_size,
                personFields=fields).execute()
        else:
            results = contacts_service.people().connections().list(
                resourceName=resource,
                pageSize=page_size,
                personFields=fields,
                pageToken=next_page_token).execute()

        result_contacts = results.get('connections', [])
        goog_contacts += result_contacts

        # Safely retrieve the next page token, if it exists
        next_page_token = results.get('nextPageToken', None)

        # Stop iterating if there's no next page token
        keep_iterating = next_page_token is not None

    return goog_contacts

def get_google_id(goog_contacts):
    # goog_contacts = get_google_contact()
    goog_id_map = []
    for contact in goog_contacts:
        # Only process contacts where we have user defined fields. This will be our flag for the ones to modify.
        if 'userDefined' in contact:
            for field in contact['userDefined']:
                if field['key'] == 'GUID':
                    goog_id_map.append({
                        'namely_guid': field['value'],
                        'google_contact_id': contact['resourceName'],
                        'google_etag': contact['etag']
                    })
    return goog_id_map

def create_contact(profile, contact_type):
    """Creates a Google contact dictionary based on Namely data and contact type."""
    
    def add_field(container, field_name, field_value):
        """Helper function to add a field to the container if the field value is not None."""
        if field_value is not None:
            container[field_name] = field_value

    def append_to_list(container, list_name, item):
        """Helper function to append an item to a list in the container if item is not None."""
        if item is not None:
            container.setdefault(list_name, []).append(item)

    google_contact = {
        'names': [{
            'givenName': profile['FIRST_NAME'],
            'familyName': profile['LAST_NAME'],
            'displayName': profile['FULL_NAME']
        }]
    }

    # Add emailAddresses
    append_to_list(google_contact, 'emailAddresses', {
        'type': 'primary',
        'value': profile.get('EMAIL')
    } if profile.get('EMAIL') else None)

    # Add organizations
    append_to_list(google_contact, 'organizations', {
        'name': "Riverdale Poultry",
        'title': profile.get('JOB_TITLE'),
        'department': profile.get('GROUP_NAME')
    } if profile.get('GROUP_NAME') else None)

    # Initialize phoneNumbers and add phone numbers
    google_contact['phoneNumbers'] = []
    append_to_list(google_contact, 'phoneNumbers', {
        'type': 'mobile',
        'value': profile.get('MOBILE_PHONE'),
        'metadata': {'primary': True}
    } if profile.get('MOBILE_PHONE') else None)

    append_to_list(google_contact, 'phoneNumbers', {
        'type': 'Office',
        'value': profile.get('OFFICE_DIRECT_DIAL')
    } if profile.get('OFFICE_DIRECT_DIAL') else None)

    # Add memberships and userDefined based on contact type
    if contact_type == 'standard':
        add_field(google_contact, 'memberships', [{
            'contactGroupMembership': {
                'contactGroupResourceName': 'contactGroups/' + std_label_id
            }
        }])

        add_field(google_contact, 'userDefined', [
            {'key': 'work_location', 'value': profile.get('GROUP_NAME')},
            {'key': 'GUID', 'value': profile.get('ID') + '-Standard'}
        ])

    elif contact_type == 'executive':
        add_field(google_contact, 'memberships', [{
            'contactGroupMembership': {
                'contactGroupResourceName': 'contactGroups/' + exec_label_id
            }
        }])

        add_field(google_contact, 'userDefined', [
            {'key': 'work_location', 'value': profile.get('GROUP_NAME')},
            {'key': 'GUID', 'value': profile.get('ID') + '-Exec'}
        ])

        append_to_list(google_contact, 'phoneNumbers', {
            'type': 'home',
            'value': profile.get('HOME_PHONE')
        } if profile.get('HOME_PHONE') else None)

        # Add birthdays
        if profile.get('DOB'):
            birthday = profile['DOB']
            add_field(google_contact, 'birthdays', [{
                'date': {
                    'year': birthday.year,
                    'month': birthday.month,
                    'day': birthday.day
                }
            }])

        # Add addresses
        append_to_list(google_contact, 'addresses', {
            'type': 'Home',
            'city': profile.get('CITY'),
            'streetAddress': profile.get('STREETADDRESS'),
            'postalCode': profile.get('POSTALCODE'),
            'countryCode': profile.get('COUNTRYCODE'),
            'region': profile.get('REGION')
        } if profile.get('STREETADDRESS') else None)

        # Add personal email
        append_to_list(google_contact, 'emailAddresses', {
            'type': 'personal',
            'value': profile.get('PERSONAL_EMAIL')
        } if profile.get('PERSONAL_EMAIL') else None)

        # Add family info as relations
        append_to_list(google_contact, 'relations', profile.get('FAMILY_INFO'))

    return google_contact
        
def create_update_contact_list(namely_data, google_contact_guid, google_field_update_list):
    """
    Creates or updates Google contact lists based on employee status. Args: namely_data (list), google_contact_guid (list), google_field_update_list (list). Returns: update_list, create_list.
    """
    update_list = {"contacts": {}, "updateMask": google_field_update_list, "readMask": google_field_update_list}
    update_contacts = {}
    create_list = {"contacts": [], "readMask": google_field_update_list}

    for profile in namely_data:
        # List to hold contacts to push to Google
        contacts_to_push = []
        status = profile.get("ACTIVE_EMPLOYEE", None)

        # Determine which contacts to create or update
        if status is True:
            contacts_to_push.append(create_contact(profile, "standard"))
            contacts_to_push.append(create_contact(profile, "executive"))
        elif status is False:
            contacts_to_push.append(create_contact(profile, "executive"))

        for contact in contacts_to_push:
            # Extract contact GUID from user-defined fields
            contact_guid = next((field["value"] for field in contact.get("userDefined", []) if field["key"] == "GUID"), None)

            # Check for matching GUID in existing Google contacts
            matching_id_pairs = search_dict_list("namely_guid", contact_guid, google_contact_guid)

            if matching_id_pairs:
                # Update contact if matching GUID is found
                contact["etag"] = matching_id_pairs[0]["google_etag"]
                contact["resourceName"] = matching_id_pairs[0]["google_contact_id"]
                update_contacts[contact["resourceName"]] = contact
            else:
                # Create contact if no matching GUID is found
                create_contacts = {"contactPerson": contact}
                create_list["contacts"].append(create_contacts)

    update_list["contacts"] = update_contacts

    # remove from live version
    with open('update_list.json', 'w') as f:
        json.dump(update_list, f)
    with open('create_list.json', 'w') as f:
        json.dump(create_list, f)

    return update_list, create_list
 
def create_remove_inactive_contact_list(google_contact_guid, namely_data):
    # determine the contact in standard list is still active, if not, then add them to the inactive list
    # find namely inactive contacts
    namely_inactive_profile = []
    for profile in namely_data:
        if profile['ACTIVE_EMPLOYEE'] == False:
            namely_inactive_profile.append(profile)

    inactive_list = {"resourceNames": []}
    for guid in google_contact_guid:
        google_guid = None
        for key, value in guid.items():
            if key == 'namely_guid':
                google_guid = value.replace('-Standard', '')
            matching_id_pair = search_dict_list('ID', google_guid, namely_inactive_profile)
            if len(matching_id_pair) > 0 and '999-999-999' not in google_guid:
                if guid['google_contact_id'] in inactive_list['resourceNames']:
                    pass
                else:
                    inactive_list['resourceNames'].append(guid['google_contact_id'])

    with open('delete_list.json', 'w') as f:
        json.dump(inactive_list, f)

    return inactive_list

def bulk_delete_contacts(resources):
    # delete the contacts in bulk, limit to 500 contacts in a single request
    # request_body ={
    #     "resourceNames": resources
    # }
    creds = authenticate_google()
    contacts_service = build('people', 'v1', credentials=creds)
    contacts_service.people().batchDeleteContacts(
        body=resources).execute()

def bulk_update_contacts(contact_update_map):
    # to bulk update contacts, limit to 200 contacts in a single request
    creds = authenticate_google()
    contacts_service = build('people', 'v1', credentials=creds)

def bulk_create_contacts(contact_create_map):
    # to bulk create contacts, limit to 200 contacts in a single request
    creds = authenticate_google()
    contacts_service = build('people', 'v1', credentials=creds)
    contacts_service.people().batchCreateContacts(
        body=contact_create_map
    ).execute()
    print("bulk create completes")

def send_error_email_gmail(subject, body_text):
    """
    Sends an email using the Gmail API when an error is encountered.

    Parameters:
        subject (str): The subject of the email.
        body_text (str): The body text of the email.
    """
    try:
        # Authenticate and obtain the Google credentials
        creds = authenticate_google()
        service = build('gmail', 'v1', credentials=creds)

        # Prepare the email content
        message = {
            'raw': base64.urlsafe_b64encode(
                f"To: pardeeps@riverdalepoultry.ca\nSubject: {subject}\n\n{body_text}".encode("utf-8")
            ).decode("utf-8")
        }

        # Send the email
        send_message = service.users().messages().send(userId="me", body=message).execute()
        print(f"Email sent successfully! Message ID: {send_message['id']}")

    except Exception as e:
        print(f"Error sending email through Gmail API: {e}")

def main(user, password):
    # Establish connection to Snowflake
    conn_params = get_connection_params(user, password)
    conn = connect_to_snowflake(conn_params)
    
    if not conn:
        print("Failed to connect to Snowflake.")
        return

    # Retrieve data from Snowflake
    query_namely = "SELECT * FROM RPE_APOLLO.NAMELY.GOOGLE_CONTACT_SCRIPT_VIEW;"
    profile_table_data, profile_field_names = retrieve_profile_data(conn, query_namely)
    dict_result = convert_to_dict(profile_table_data, profile_field_names)

    # Retrieve Google contacts
    google_contacts = get_google_contact()
    google_contact_guid = get_google_id(google_contacts)

    # Create and update contact lists
    update_list, create_list = create_update_contact_list(dict_result, google_contact_guid, google_field_update_list)
    inactive_bulk_list = create_remove_inactive_contact_list(google_contact_guid, dict_result)

    # Check and perform bulk delete for inactive contacts
    if inactive_bulk_list['resourceNames']:
        print("Bulk delete standard contacts starts")
        bulk_delete_contacts(inactive_bulk_list)

    # Check and perform bulk update for contacts
    if update_list['contacts']:
        print("Bulk update starts")
        batch_size = 199
        contacts = update_list['contacts']
        
        # Partition the contacts into batches for bulk update
        contact_batches = [{'contacts': {contact_id: contacts[contact_id] for contact_id in list(contacts.keys())[i:i+batch_size]}} 
                           for i in range(0, len(contacts), batch_size)]

        for batch in contact_batches:
            batch.update({'updateMask': google_field_update_list, 'readMask': google_field_update_list})
            bulk_update_contacts(batch)
        print("bulk update completes")

    # Check and perform bulk create for contacts
    if create_list['contacts']:
        print("Bulk create starts")
        bulk_create_contacts(create_list)

    send_error_email_gmail("Lambda Function Error", "Test Email")

if __name__ == "__main__":
    main('AWS_FS01_SYNC_USER', '7RLB$os?RedCJE8H')