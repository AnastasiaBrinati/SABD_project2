# *****************************************
# Legge il file csv del dataset in locale
# simula gli arrivi delle tuple come stream
# *****************************************

import sys
import csv

import producer


def read_csv(filename):
    try:
        producer.connect()
    except Exception as e:
        print(f"Error connecting to Nifi: {e}")
    try:
        with open(filename, 'r', newline='') as file:
            file = csv.reader(file)
            for row in file:
                print(f"row: {row}")
                producer.send_to_nifi(row)
    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py nome_file_csv")
    else:
        file_csv = sys.argv[1]
        read_csv(file_csv)
