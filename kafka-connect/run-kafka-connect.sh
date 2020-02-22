#!/usr/bin/env bash

cd /Users/tmoisiuk/IdeaProjects/in-stream-project/kafka-connect

docker-compose up kafka-cluster

docker run --rm -it -v "$(pwd)":/bot-detection --net=host landoop/fast-data-dev:cp3.3.0 bash

cd /bot-detection

kafka-topics --create --topic click-stream --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

connect-standalone worker.properties bot-detection-stream.properties

#tmoisiuk@C6507 kafka_2.12-2.2.1 % bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic click-stream --from-beginning