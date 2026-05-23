#!/bin/bash
docker compose -f docker-compose-hadoop.yaml down
docker compose -f docker-compose-kafka.yaml down

rm -rf dashboard/data/*
rm -rf .rss_seen_ids.json