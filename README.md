# SABD_project2

# Data Monitoring Analysis with Apache Flink

![Python](https://img.shields.io/badge/Python-v3.11-blue.svg?logo=python&longCache=true&logoColor=white&colorB=5e81ac&style=flat-square&colorA=4c566a)
![GitHub Last Commit](https://img.shields.io/github/last-commit/google/skia.svg?style=flat-square&colorA=4c566a&colorB=a3be8c&logo=GitHub)

The purpose of this project is [..]

The dataset contains S.M.A.R.T monitoring data, extended with some attributes captured by Backblaze. It includes events regarding around 200k hard disks, where each event reports the S.M.A.R.T. status of a particular hard disk on a specific day. The reduced dataset contains approximately 3 million events (compared to the 5 million in the original dataset).

## Project Queries

#### Q1

- For each day, for each vault (refer to the vault id field), calculate the total number of failures. Determine the list of vaults that have experienced exactly 4, 3, and 2 failures.

#### Q2

- Calculate the top 10 models of hard disks that have experienced the highest number of failures. The ranking should include the hard disk model and the total number of failures suffered by hard disks of that specific model. Then, compute a second ranking of the top 10 vaults that have recorded the highest number of failures. For each vault, report the number of failures and the list (without repetitions) of hard disk models subject to at least one failure.

#### Q3

- Compute the minimum, 25th, 50th, 75th percentile, and maximum of the operating hours (s9 power on hours field) of hard disks that have experienced failures and hard disks that have not experienced failures. Pay attention, the s9 power on hours field reports a cumulative value, therefore the statistics required by the query should refer to the last useful detection day for each specific hard disk (consider the use of the serial number field). Also, indicate the total number of events used for calculating the statistics in the output.

## Usage

### Starting the Overall Architecture

To start the overall architecture run the script with the dataset path parameter:

   ```bash
   ./setup-architecture.sh --start
   ```
   ```

For further details of the script, execute:

```bash
./setup-architecture.sh --help
```

### Running the Producer

Once the architecture has started, to run the producer of the tuples, follow these steps:

1. Start the producer container by running:

   ```bash
   ./produce.sh
   ```

   This command will provide you with a shell into the client container.
