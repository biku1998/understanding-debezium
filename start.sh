#!/bin/bash

docker compose down

rm -rf ./analytics-pgdata
rm -rf ./transactional-pgdata

docker compose up -d --build --force-recreate