#!/bin/bash

RAY_HEAD_IP=$1

echo "Reconnecting to head $RAY_HEAD_IP..."


source activate tensorflow_p36 && ray stop
pkill -9 ray || true
source activate tensorflow_p36 && ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076
