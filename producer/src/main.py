# *****************************************
# Legge il file csv del dataset in locale
# simula gli arrivi delle tuple come stream
# *****************************************

import csv
import producer

# Specifica il percorso del file CSV
CSV_FILE_PATH = './data/file.csv'


def read_csv(filename):
    try:
        producer.connect()
    except Exception as e:
        print(f"Error connecting to Nifi: {e}")
    try:
        with open(filename, 'r', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                print(f"row: {row}")
                producer.send_to_nifi(row)
    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")


if __name__ == "__main__":
    read_csv(CSV_FILE_PATH)
