#!/bin/bash

docker build ./producer -t producer
docker run -t -i -p 4040:4040 --network project2-network --name=producer --volume ./producer/src:/home/producer/src --workdir /home/producer --env-file ./producer/src/.env producer