#!/bin/bash

# up docker
docker compose -f docker-compose-hadoop.yaml up -d
docker compose -f docker-compose-kafka.yaml up -d

sleep 30

# kafka topics
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic saham-api
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic saham-rss
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# hdfs folder
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/saham/api
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/saham/rss
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/saham/hasil
docker exec -it hadoop-namenode hdfs dfs -chown -R 777 /data/saham
docker exec -it hadoop-namenode hdfs dfs -ls /data/saham