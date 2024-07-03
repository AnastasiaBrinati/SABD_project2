# *****************************************
# Legge il file csv del dataset in locale
# simula gli arrivi delle tuple come stream
# *****************************************

import csv
import producer

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
            for row in reader:
                # for debugging
                print(f"row: {row}")

                # send rows to nifi
                try:
                    producer.send_to_nifi(row)
                except Exception as e:
                    print(f"Error sending data to Nifi: {e}")

    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")




if __name__ == "__main__":
    start_production()
