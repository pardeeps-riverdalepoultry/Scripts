##############################################
# Import
##############################################
import requests
import json
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
import os
from datetime import datetime
import math
import csv
##############################################
# Config
##############################################
# Namely API
namely_profiles_url = r'''https://riverdalepoultry.namely.com/api/v1/profiles'''
namely_api_token = 'jwgtb0xqQEmscs9lbHWDbFEKbQkzzrkYXSb8e1zCsoroLUFc7jkOW5ezcX5ov0zY'
namely_groups_url = r'''https://riverdalepoultry.namely.com/api/v1/groups'''
namely_location_group_type_id = '5bc7ad74-b8e6-4975-bc2b-6a4ae62e065a'
request_headers = {'Authorization': 'Bearer ' + namely_api_token, 'Accept': 'application/json'}
profiles_per_page = 40
url_filter_string = '?&per_page=' + str(profiles_per_page)

SCOPES = [
    'https://www.googleapis.com/auth/admin.directory.user',  # Admin SDK scope
    'https://www.googleapis.com/auth/contacts'  # Google Contacts scope
]
# Google contacts labels
std_label_id = '52893d4a0d6d4643'
exec_label_id = '639c56d38f4616ed'
google_field_update_list = 'names,memberships,emailAddresses,phoneNumbers,relations,userDefined,organizations,birthdays,addresses'
#google_field_short_update = 'names,memberships,emailAddresses,phoneNumbers,userDefined,organizations'


####Data Import Functions####
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

def get_namely_profile_data():
    # get all namely profile data
    namely_profiles = []

    # Get profile data from Namely, page 1
    print('Retrieving profile data from Namely, page 1')
    response = requests.get(namely_profiles_url + url_filter_string, headers=request_headers)
    response_data = json.loads(response.content.decode('utf-8'))

    # Calculate total number of pages we'll need to process
    #------ num_pages = int(math.ceil(response_data['meta']['total_count'] / profiles_per_page))
    num_pages = 1
    for i in range(1, num_pages + 1):
         # First iteration we don't need to hit Namely API for another page. Already have the first call completed
         if i != 1:
             print('Retrieving profile data from Namely, page ' + str(i))
             url = namely_profiles_url + url_filter_string + '&page=' + str(i)
             response_data = json.loads(requests.get(url, headers=request_headers).content.decode('utf-8'))

    #     # Add retrieved profile data from Namely to master list
         namely_profiles.extend(response_data['profiles'])

    return namely_profiles

def get_namely_group_data(group_type_id):
    # Retrieves list of entries under a user-defined group in Namely (Work location, department, division, etc)
    # Requires the identifier of the group to make the query.

    group_response = requests.get(namely_groups_url, headers=request_headers)
    group_data = json.loads(group_response.content.decode('utf-8'))

    group_member_ids = []
    for i in range(0, len(group_data['groups'])):
        if group_data['groups'][i]['links']['group_type'] == group_type_id:
            group_member_ids.append(group_data['groups'][i]['id'])

    return group_member_ids

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

####Utility Functions####
def not_empty(val):
    if None != val and val != '':
        return True
    else:
        return False

def search_dict_list(search_key, search_value, dic_list):
    # Retrieves items from a list of dictionaries based on a desired key and value to search for in the dictionaries
    return [element for element in dic_list if element[search_key] == search_value]

def create_contact(namely_profile, contact_type, work_location_ids):
    # Create employee name - if they have a preferred first name in their profile, replace their first name with it.
    preferred_name = namely_profile['preferred_name']
    if not_empty(preferred_name):
        first_name = preferred_name
    else:
        first_name = namely_profile['first_name']

    name = first_name + " " + namely_profile['last_name']

    # Add fields available to both standard and exec lists
    goog_contact = {
        'names': [{
            'givenName': first_name,
            'familyName': namely_profile['last_name'],
            'displayName': name
        }],
        'emailAddresses': [{'type': 'primary', 'value': namely_profile['email']}],
        'organizations': [{
            'name': 'Riverdale Poultry',
            'title': namely_profile['job_title']['title']
        }],
        'phoneNumbers': [],
        'userDefined': []
    }

    # Search profile groups for work location and add to user-defined contact fields
    for group in namely_profile['links']['groups']:
        if group['id'] in work_location_ids:
            goog_contact['userDefined'].append({
                'key': 'Work Location',
                'value': group['name']
            })

            goog_contact['organizations'][0]['location'] = group['name']

    # If employee is inactive, do not list mobile number. We recycle RPE phones, so can cause issues with
    # new hires appearing as old employees in people's contacts
    if namely_profile['user_status'] == 'active':
        goog_contact['phoneNumbers'].append({
            'type': 'mobile',
            'value': namely_profile['mobile_phone'],
            'metadata': {'primary': True}
        })

    # Office direct dial
    office_phone = namely_profile['office_direct_dial']
    if not_empty(office_phone):
        goog_contact['phoneNumbers'].append({
            'type': 'Office',
            'value': office_phone
        })

    # Standard vs exec contact list - apply labels and add extra fields
    if contact_type == 'standard':
        # Contact label
        goog_contact['memberships'] = [
            {'contactGroupMembership': {'contactGroupResourceName': 'contactGroups/' + std_label_id}}]

        # Namely GUID
        goog_contact['userDefined'].append({'key': 'GUID', 'value': namely_profile['id'] + '-Standard'})

    elif contact_type == 'executive':
        # Contact label
        goog_contact['memberships'] = [
            {'contactGroupMembership': {'contactGroupResourceName': 'contactGroups/' + exec_label_id}}]

        # Namely GUID
        goog_contact['userDefined'].append({'key': 'GUID', 'value': namely_profile['id'] + '-Exec'})

        # Home phone
        home_phone = namely_profile['home_phone']
        if not_empty(home_phone):
            goog_contact['phoneNumbers'].append({'type': 'home', 'value': home_phone})

        # Birthday
        dob = namely_profile['dob']
        if not_empty(dob):
            birthday = datetime.strptime(dob, '%Y-%m-%d')
            goog_contact['birthdays'] = [{'date': {
                'day': birthday.day,
                'month': birthday.month,
                'year': birthday.year
            }}]

        # Address block
        address = namely_profile['home']
        goog_contact['addresses'] = [{
            'type': 'Home',
            'city': address['city'],
            'streetAddress': address['address1'],
            'postalCode': address['zip'],
            'countryCode': address['country_id'],
            'region': address['state_id']
        }]

        # Family Members
        goog_contact['relations'] = []
        for i in range(1, 11):

            if not_empty([str(i) + '_name']):
                goog_contact['relations'].append({
                    'person': namely_profile[str(i) + '_name'],
                    'type': ','.join(namely_profile[str(i) + '_family_relation'])
                })

        # Personal Email
        personal_email = namely_profile['personal_email']
        if not_empty(personal_email):
            goog_contact['emailAddresses'].append({
                'type': 'personal',
                'value': personal_email
            })

    return goog_contact
####Bulk Action Functions####
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


    contacts_service.people().batchUpdateContacts(
        body=contact_update_map,
    ).execute()
    print("bulk update completes")

def bulk_create_contacts(contact_create_map):
    # to bulk create contacts, limit to 200 contacts in a single request
    creds = authenticate_google()
    contacts_service = build('people', 'v1', credentials=creds)
    contacts_service.people().batchCreateContacts(
        body=contact_create_map
    ).execute()
    print("bulk create completes")

def create_update_list(namely_data, work_location_ids, good_contact_guid, google_field_update_list):
    update_list = {"contacts": {}, "updateMask": google_field_update_list, 'readMask': google_field_update_list}
    update_contacts = {}
    create_list = {"contacts": [], 'readMask': google_field_update_list}
    i = 0
    for profile in namely_data:

        # Populate list of contacts to push to Google based on employee status in Namely
        # Active employees go to both lists, inactive only goes to exec
        # For now omit pending employees. They probably don't matter.
        create_contacts = {}
        contacts_to_push = []
        status = profile['user_status']
        if status == 'active':
            contacts_to_push.append(create_contact(profile, 'standard', work_location_ids))
            contacts_to_push.append(create_contact(profile, 'executive', work_location_ids))
        elif status == 'inactive':
            contacts_to_push.append(create_contact(profile, 'executive', work_location_ids))
        else:
            pass

        # Push contacts to Google
        for contact in contacts_to_push:

            # Check for matching GUID in existing contacts
            contact_guid = None
            for field in contact['userDefined']:
                if field['key'] == 'GUID':
                    contact_guid = field['value']

            matching_id_pairs = search_dict_list('namely_guid', contact_guid, good_contact_guid)

            # construct a list of update list and create list
            if len(matching_id_pairs) > 0:
                contact['etag'] = matching_id_pairs[0]['google_etag']
                contact['resourceName'] = matching_id_pairs[0]['google_contact_id']
                update_contacts.update({contact['resourceName']: contact})

            else:
                create_contacts.update({"contactPerson": contact})
                create_list["contacts"].append(create_contacts)
    update_list["contacts"] = update_contacts

    with open('update_list.json', 'w') as f:
        json.dump(update_list, f)
    with open('create_list.json', 'w') as f:
        json.dump(create_list, f)
        
    return update_list, create_list
    # save update_list and create_list to a json file
   
def create_remove_inactive_contact_list(google_contact_guid, namely_data):
    # determine the contact in standard list is still active, if not, then add them to the inactive list
    # find namely inactive contacts
    namely_inactive_profile = []
    for profile in namely_data:
        profile = profile[0]
        if profile['ACTIVE_EMPLOYEE'] == False:
            namely_inactive_profile.append(profile)

    inactive_list = {"resourceNames": []}
    for guid in google_contact_guid:
        google_guid = None
        for key, value in guid.items():
            if key == 'namely_guid':
                google_guid = value.replace('-Standard', '')
            matching_id_pair = search_dict_list('id', google_guid, namely_inactive_profile)
            if len(matching_id_pair) > 0 and '999-999-999' not in google_guid:
                if guid['google_contact_id'] in inactive_list['resourceNames']:
                    pass
                else:
                    inactive_list['resourceNames'].append(guid['google_contact_id'])
    return inactive_list


if __name__ == '__main__':
    # Gathering data from sources
    namely_data = get_namely_profile_data()
    goog_contacts = get_google_contact()

    # Extracting information from the data
    google_contact_guid = get_google_id(goog_contacts)
    work_location_ids = get_namely_group_data(namely_location_group_type_id)

    # Creating lists to feed google's contact api e
    update_list, create_list = create_update_list(namely_data, work_location_ids, google_contact_guid, google_field_update_list)
    inactive_bulk_list = create_remove_inactive_contact_list(google_contact_guid, namely_data)

    update_list_len = len(update_list['contacts'])
    create_list_len = len(create_list['contacts'])
    inactive_list_len = len(inactive_bulk_list['resourceNames'])

    if inactive_list_len > 0:
        print("bulk delete standard contacts starts")
        bulk_delete_contacts(inactive_bulk_list)
    else:
        pass

    if update_list_len > 0:
        print("bulk update starts")
        # The update list should be partitioned into separate batches because the Google Contacts API has a limit of 200 contacts for each bulk operation.
        batch_size = 199
        contacts = update_list['contacts']
        contact_batches = []

        for i in range(0, len(contacts), batch_size):
            batch = {'contacts': {}}
            for contact_id in list(contacts.keys())[i:i+batch_size]:
                batch['contacts'][contact_id] = contacts[contact_id]
            contact_batches.append(batch)

        for batch in contact_batches:
            batch['updateMask'] = google_field_update_list
            batch['readMask'] = google_field_update_list
            # sending batch to google's bulk update operation
            bulk_update_contacts(batch)
    else:
        pass

    if create_list_len > 0:
        print("bulk create starts")
        bulk_create_contacts(create_list)

