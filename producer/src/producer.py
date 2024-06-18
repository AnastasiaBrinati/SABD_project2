import requests
import os
from utils.nifi import NifiApi


def send_to_nifi(row):
    url = 'http://nifi:' + os.getenv('LISTEN_HTTP_PROCESSOR_PORT') + '/contentListener'
    headers = {'Content-Type': 'application/json'}
    payload = row  # Sending the CSV row as a JSON payload

    print("Sending row to NiFi ... ")
    try:
        response = requests.post(url, json=payload, headers=headers, verify=False)  # verify=False to ignore SSL warnings
        if response.status_code == 200:
            print(f"Successfully sent data to NiFi: {row}")
        else:
            print(f"Failed to send data to NiFi: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending data to NiFi: {e}")


def connect() -> None:
    # Starts the ingestion flow, by triggering the flow of a nifi processor group.

    # Get nifi credentials from environment variables
    username = os.getenv('NIFI_USERNAME')
    password = os.getenv('NIFI_PASSWORD')
    if password is None:
        raise Exception('NIFI_PASSWORD is required')

    # Initialize the nifi api, login and schedule the processor group
    NifiApi.init_api().login(username=username, password=password).schedule_ingestion()
