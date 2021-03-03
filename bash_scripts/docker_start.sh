#!/bin/bash

path=$(dirname $(realpath "${BASH_SOURCE[0]}"))
echo "Starting Trompa crowdsourcing backend in ${MODE} mode..."
if [ "$MODE" == "TEST" ]
then
    mv settings_test.yaml settings.yaml
    source $path/docker_test_start.sh
elif [ "$MODE" == "LIVE" ]
then
    mv settings_live.yaml settings.yaml
    source $path/docker_live_start.sh
fi