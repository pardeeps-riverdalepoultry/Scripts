#########################
# Import libraries
#########################
import pyodbc
import json
import requests
import time
import pandas as pd

# ***********************
# MS Access
# ***********************
merlin_file_path = r'''\\m1cdfs001\Riverdale\20.0 Project Merlin\20.1 ETL System - Weekly Process\_externals\1\merlin.accdb'''

db_connection_string = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + merlin_file_path
)

# ***********************
# Fleetio API
# ***********************
fleetio_api_token = '1a8c31b36d9b66e8a77b143234b3d6df3ceeb323'
accountToken = '4637ceb20d'
request_headers = {'Authorization': 'Token ' + fleetio_api_token, "Account-Token": accountToken,
                   'Accept': 'application/json'}

# ***********************
# functions
# ***********************

def get_contacts():
    api_root = f"https://secure.fleetio.com/api/v2/contacts"
    
    response = requests.get(api_root, headers=request_headers)
    returned_contacts = response.json()
    return returned_contacts


# ***********************
# Main Function
# ***********************
if __name__ == "__main__":
    contacts = get_contacts()
    print(contacts)


