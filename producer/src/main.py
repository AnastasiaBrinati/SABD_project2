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
            current_date = datetime.strptime(first_row[0][:10], "%Y-%m-%d").date()
            print("Starting to send rows")
            producer.send_to_nifi(first_row)

            for row in reader:
                # for debugging
                #print(f"row: {row[0][:10]}")

                timestamp = datetime.strptime(row[0][:10], "%Y-%m-%d").date()
                year = timestamp.year
                month = timestamp.month
                day = timestamp.day

                if timestamp > current_date:
                    # fake waiting
                    print("finished one day events")
                    time.sleep(5)
                    current_date = timestamp
                try:
                    # send row to nifi
                    producer.send_to_nifi(row)
                except Exception as e:
                    print(f"Error sending data to Nifi: {e}")

    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")


if __name__ == "__main__":
    start_production()
