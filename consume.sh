#!/bin/bash

docker stop taskmanager1
docker rm taskmanager1
docker stop taskmanager2
docker rm taskmanager2
docker stop jobmanager
docker rm jobmanager

# Build the Docker images
docker build -t flink-jobmanager-image -f ./consumer/jobmanager.dockerfile ./consumer/
docker build -t flink-taskmanager-image -f ./consumer/taskmanager.dockerfile ./consumer/

# Run the JobManager container
docker run -d --name=jobmanager --network project2-network -p 8081:8081 -p 6123:6123 \
  --volume ./consumer/src:/src \
  --workdir /opt/flink/ \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager
  taskmanager.numberOfTaskSlots: 4" \
  flink-jobmanager-image \
  jobmanager

# Pause to allow JobManager to start
echo "Waiting for JobManager to start..."

# Run the TaskManager containers
docker run -d --name=taskmanager1 --network project2-network \
  -e "FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager" \
  flink-taskmanager-image \
  taskmanager

docker run -d --name=taskmanager2 --network project2-network \
  -e "FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager" \
  flink-taskmanager-image \
  taskmanager

echo "Flink JobManager and TaskManager containers are up and running."

docker exec -t -i jobmanager bash
