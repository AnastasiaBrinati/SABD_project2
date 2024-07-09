# *****************************************
# Legge il file csv del dataset in locale
# simula gli arrivi delle tuple come stream
# *****************************************

import csv
import producer
from datetime import datetime
import time


# Specifica il percorso del file CSV
CSV_FILE_PATH = './src/data/file.csv'


def start_production():
    # try connecting to nifi
    try:
        producer.connect()
    except Exception as e:
        print(f"Error connecting to Nifi: {e}")

    # read rows from csv:
    try:
        with open(CSV_FILE_PATH, 'r', newline='') as file:
            reader = csv.reader(file)

            days_count = 0
            count = 0

            # Save the first row as header
            header = next(reader)
            first_row = next(reader)
            print(f"first_row: {first_row}")

            current_date = datetime.strptime(first_row[0][:10], "%Y-%m-%d").date()
            print("Starting to send rows")
            producer.send_to_nifi(first_row)

            count += 1

            for row in reader:
                try:
                    timestamp = datetime.strptime(row[0][:10], "%Y-%m-%d").date()
                except ValueError as e:
                    print(f'ValueError: {e}')
                    continue

                if timestamp > current_date:
                    # fake waiting
                    days_count += 1
                    count = 0
                    print(f"finished day {days_count} events")
                    time.sleep(5)
                    current_date = timestamp
                try:
                    # send row to nifi
                    count += 1
                    if count < 200:
                        producer.send_to_nifi(row)
                except Exception as e:
                    print(f"Error sending data to Nifi: {e}")
                    continue

            last_row = ['2060-12-12T00:00:00.000000', '8HK2SSMH', 'HGST HUH721212ALN604', '0', '1113', '0.0', '96.0', '396.0', '24.0', '0.0', '0.0', '18.0', '38445.0', '0.0', '24.0', '', '', '', '', '', '', '', '', '1613.0', '1613.0', '31.0', '', '0.0', '0.0', '0.0', '0.0', '', '', '', '', '', '', '', '']
            print("sending the last row")
            producer.send_to_nifi(last_row)

    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")


if __name__ == "__main__":
    start_production()
