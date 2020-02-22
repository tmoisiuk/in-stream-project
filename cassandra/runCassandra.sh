#!/usr/bin/env bash

docker network create cassandranetwork

docker run --name some-cassandra --network cassandranetwork -p 7000:7000 -p 9042:9042 -d cassandra:latest
docker run -it --network cassandranetwork --rm cassandra cqlsh some-cassandra

cqlsh -f create_table.cql

cqlsh -f select.cql