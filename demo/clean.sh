#!/bin/bash

if [ "$1" == "--all" ]; then
    docker compose down --rmi all
    rm -rf grafana/persistent
    rm -rf influxdb/persistent/influxdb/*
else
    docker compose down
fi