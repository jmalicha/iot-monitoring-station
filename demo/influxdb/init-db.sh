#!/bin/sh

# Execute an InfluxDB command to create a new database named "iot_monitoring_station"
# with an infinite retention policy, meaning data will never be automatically deleted.
influx -execute 'CREATE DATABASE "iot_monitoring_station" WITH DURATION INF'