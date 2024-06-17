#!/bin/bash

docker build ./producer -t producer
cd ../
docker run -t -i -p 4040:4040 --network project2-network --name=producer --volume ./producer/src:/home/producer/src producer
