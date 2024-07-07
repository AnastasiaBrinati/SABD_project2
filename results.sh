#!/bin/bash

# Rimuovi il container esistente, se presente
docker stop results_consumer
docker rm results_consumer

# Costruisci l'immagine Docker
docker build ./results -t results_consumer

# Esegui il container Docker con il file CSV montato come volume
docker run -t -i -p 4041:4041 \
  --network project2-network \
  --name=results_consumer \
  --volume ./results/src:/home/results/src \
  --workdir /home/results/src/ \
  results_consumer
