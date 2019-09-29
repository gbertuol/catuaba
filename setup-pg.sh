#!/bin/bash

docker run --rm --name pg-docker -e POSTGRES_PASSWORD=docker  -d  -p 5432:5432  -v /home/bertuol/data:/var/lib/postgresql/data  postgres:latest  -c 'max_wal_senders=1'  -c 'wal_keep_segments=1'  -c 'wal_level=logical'  -c 'max_replication_slots=4'