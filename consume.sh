#!/bin/bash

docker stop taskmanager
docker rm taskmanager
docker stop jobmanager
docker rm jobmanager

# Build the Docker images
docker build -t flink-jobmanager-image -f ./flink/jobmanager.dockerfile ./flink/
docker build -t flink-taskmanager-image -f ./flink/taskmanager.dockerfile ./flink/

# Run the JobManager container
docker run -d --name jobmanager --network project2-network -p 8081:8081 -p 6123:6123 \
  -v ./Results:/Results \
  -v ./flink/src:/src \
  -e "FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager" \
  flink-jobmanager-image \
  jobmanager

# Pause to allow JobManager to start
echo "Waiting for JobManager to start..."
sleep 20

# Run the TaskManager container
docker run -d --name taskmanager --network project2-network -v ./Results:/Results \
  -e "FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager" \
  flink-taskmanager-image \
  taskmanager

echo "Flink JobManager and TaskManager containers are up and running."

docker exec -t -i jobmanager bash
