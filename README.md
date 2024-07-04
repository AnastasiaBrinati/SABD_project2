# SABD_project2

# Data Monitoring Analysis with Apache Flink

![Python](https://img.shields.io/badge/Python-v3.11-blue.svg?logo=python&longCache=true&logoColor=white&colorB=5e81ac&style=flat-square&colorA=4c566a)
![GitHub Last Commit](https://img.shields.io/github/last-commit/google/skia.svg?style=flat-square&colorA=4c566a&colorB=a3be8c&logo=GitHub)

The purpose of this project is [..]

The dataset contains S.M.A.R.T monitoring data, extended with some attributes captured by Backblaze. It includes events regarding around 200k hard disks, where each event reports the S.M.A.R.T. status of a particular hard disk on a specific day. The reduced dataset contains approximately 3 million events (compared to the 5 million in the original dataset).

## Project Queries

#### Q1

- ...

#### Q2

- ...

#### Q3

- ...

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

Once the architecture has started, to run the consumer (flink cluster consisting of one jobmanager and one taskamanager), follow these steps:

1. Start the consumer containers by running:

   ```bash
   ./consume.sh
   ```
   This command will provide you with a shell into the client container.
   
2. Make sure you are in the opt/flink/src wordir and start consuming:
   
   ```bash
   python consumer.py [ q1 | q2 | q3 ] [ 1 | 3 | all ]
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
