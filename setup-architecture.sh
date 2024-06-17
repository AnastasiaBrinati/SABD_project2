#!/bin/bash

# Variables
PATH_NIFI_SCRIPTS=/opt/nifi/nifi-current/conf/nifi-ingestion/scripts
NIFI_CONTAINER=nifi
DOCKER_NETWORK=project2-network

DATASET_RELATIVE_PATH=./producer/src/data

usage() {
    echo "Usage:"
    echo "       ./setup-architecture.sh --start:
                   Starts the whole architecture."
}

run_docker_compose() {
    echo "Starting the architecture..."
    docker compose up -d
}

execute() {
    if [ "$1" = "--help" ]; then
        usage
        exit 0
    elif [ "$1" = "--start" ]; then
        run_docker_compose 1
    else
        usage
    fi
}

# Execute the script
execute $@