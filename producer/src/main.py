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

            # Save the first row as header
            header = next(reader)
            first_row = next(reader)
            count = 1

            current_date = datetime.strptime(first_row[0][:10], "%Y-%m-%d").date()
            print("Starting to send rows")
            producer.send_to_nifi(first_row)

            for row in reader:
                try:
                    timestamp = datetime.strptime(row[0][:10], "%Y-%m-%d").date()
                except ValueError as e:
                    print(f'ValueError: {e}')
                    continue

                if timestamp > current_date:
                    # fake waiting
                    print("finished one day events")
                    time.sleep(5)
                    current_date = timestamp
                    count = 0
                try:
                    # send row to nifi
                    if count < 2000:
                        count += 1
                        print(str(count) + " " + str(row[0:2]))
                        producer.send_to_nifi(row)
                except Exception as e:
                    print(f"Error sending data to Nifi: {e}")
                    continue

    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")


if __name__ == "__main__":
    start_production()
