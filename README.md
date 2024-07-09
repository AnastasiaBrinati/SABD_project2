# SABD_project2

# Data Monitoring Analysis with Apache Flink

![Python](https://img.shields.io/badge/Python-v3.11-blue.svg?logo=python&longCache=true&logoColor=white&colorB=5e81ac&style=flat-square&colorA=4c566a)
![GitHub Last Commit](https://img.shields.io/github/last-commit/google/skia.svg?style=flat-square&colorA=4c566a&colorB=a3be8c&logo=GitHub)

The purpose of this project is [..]

The dataset contains S.M.A.R.T monitoring data, extended with some attributes captured by Backblaze. It includes events regarding around 200k hard disks, where each event reports the S.M.A.R.T. status of a particular hard disk on a specific day. The reduced dataset contains approximately 3 million events (compared to the 5 million in the original dataset).

## Project Queries

#### Q1

- Per i vault (campo vault id) con identificativo compreso tra 1000 e 1020, calcolare il numero di
eventi, il valor medio e la deviazione standard della temperatura misurata sui suoi hard disk (campo
s194 temperature celsius). Si faccia attenzione alla possibile presenza di eventi che non
hanno assegnato un valore per il campo relativo alla temperatura.

#### Q2

- Calcolare la classifica aggiornata in tempo reale dei 10 vault che registrano il più alto numero di falli-
menti nella stessa giornata. Per ogni vault, riportare il numero di fallimenti ed il modello e numero
seriale degli hard disk guasti.

#### Q3

- Calcolare il minimo, 25-esimo, 50-esimo, 75-esimo percentile e massimo delle ore di funzionamento
(campo s9 power on hours) degli hark disk per i vault con identificativo tra 1090 (compreso) e
1120 (compreso). Si presti attenzione, il campo s9 power on hours riporta un valore cumulativo,
pertanto le statistiche richieste dalla query devono far riferimento all’ultimo valore utile di rilevazione
per ogni specifico hard disk (si consideri l’uso del campo serial number).

## Usage

### Starting the Overall Architecture

To start the overall architecture run the script:

   ```bash
   ./setup-architecture.sh --start
   ```

For further details on the script, execute:

   ```bash
   ./setup-architecture.sh --help
   ```

### Running the Consumer

Once the architecture has started, to run the consumer (flink cluster consisting of one jobmanager and two taskmanagers), follow these steps:

1. Start the consumer containers by running:

   ```bash
   ./consume.sh
   ```
   This command will provide you with a shell into the client container.
   
2. Make sure you are in the opt/flink wordir and start consuming:
   
   ```bash
    bin/flink run --python /opt/flink/src/consumer.py --jar ./lib/flink-sql-connector-kafka-1.17.1.jar -- [ q1 | q2 | q3 ]
   ```
   
### Running the Producer

Once the architecture has started, to run the producer of the tuples, follow these steps:

1. Start the producer container by running:

   ```bash
   ./produce.sh <dataset-path>
   ```
   This command will provide you with a shell into the client container.

2. Start producing:
   
   ```bash
   python src/main.py
   ```

### Running the Result Consumer

Once a query has started, to consume the results and convert them to csv files, follow these steps:

1. Start the consumer container by running:

   ```bash
   ./results.sh
   ```
   This command will provide you with a shell into the client container.

2. Start reading:
   
   ```bash
   python results_consumer.py <topic_name>
   ```
