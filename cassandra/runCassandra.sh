#!/usr/bin/env bash

docker run --name cassandra \
    -p 7000:7000  \
    bitnami/cassandra:latest


cqlsh -f create_table.cql

cqlsh -f select.cql