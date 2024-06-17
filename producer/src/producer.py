import os
from utils.nifi import NifiApi

def send_to_nifi(row):
    url = 'http://nifi:8443/contentListener'
    headers = {'Content-Type': 'application/json'}
    payload = row  # Sending the CSV row as a JSON payload
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Successfully sent data to NiFi: {row}")
    else:
        print(f"Failed to send data to NiFi: {response.text}")

"""
def ingest() -> None:
"""
    # Executes the ingestion, by triggering the flow of a nifi processor group.
    # It acquires the data from a x and ingests it as a kafka topic, no pre-processing steps.

"""

    # Get nifi credentials from environment variables
    username = os.getenv('NIFI_USERNAME', 'admin')
    password = os.getenv('NIFI_PASSWORD')

    if password is None:
        raise Exception('NIFI_PASSWORD is required')
    # Initialize the nifi api, login and schedule the processor group
    NifiApi.init_api().login(username=username, password=password).schedule_ingestion()
    
"""