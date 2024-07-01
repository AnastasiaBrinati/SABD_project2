#!/bin/bash

# Controlla se è stato passato un argomento
if [ $# -eq 0 ]; then
    echo "No CSV file specified. Usage: $0 <path_csv_file>"
    exit 1
fi

# Ottieni il percorso assoluto del file CSV
CSV_FILE=$(realpath $1)

# Controlla se il file esiste
if [ ! -f "$CSV_FILE" ]; then
    echo "The given file does not exist: $CSV_FILE"
    exit 1
fi

# Rimuovi il container esistente, se presente
docker rm producer

# Costruisci l'immagine Docker
docker build ./producer -t producer

# Esegui il container Docker con il file CSV montato come volume
docker run -t -i -p 4040:4040 \
  --network project2-network \
  --name=producer \
  --volume ./producer/src:/home/producer/src \
  --volume "$CSV_FILE":/home/producer/src/data/file.csv \
  --workdir /home/producer/src \
  --env-file ./producer/src/.env \
  producer
