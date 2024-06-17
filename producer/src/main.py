# *****************************************
# Legge il file csv del dataset in locale
# simula gli arrivi delle tuple come stream
# *****************************************

import sys
import csv

def read_csv(filename):
    try:
        with open(filename, 'r', newline='') as file_csv:
            file = csv.reader(file_csv)
            for row in file:
                print(row)
    except FileNotFoundError:
        print(f"Error: file '{filename}' not found.")
    except Exception as e:
        print(f"Error while reading file '{filename}': {e}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python nome_script.py nome_file_csv")
    else:
        file_csv = sys.argv[1]
        read_csv(file_csv)
