#!/bin/bash


system=$1
if [ "$2" == "local" ]; then
    echo "LOCAL"
    local_prefix="local-"
    local_arg="--local"
else
    echo "REMOTE"
    local_prefix=""
    local_arg=""
fi


python ownership-nsdi21-artifact/latency-microbenchmark/latency.py --system $system $local_arg --output output.csv --pipelined
python ownership-nsdi21-artifact/latency-microbenchmark/latency.py --system $system $local_arg --output output.csv --use-actors --pipelined
