#!/bin/bash


source activate tensorflow_p36
output=$1
echo "Writing output to $output"

SYSTEM_ARG=$2

for delay in 1000 500 100 50 10; do
    python ownership-nsdi21-artifact/recovery-microbenchmark/reconstruction.py $SYSTEM_ARG --delay-ms $delay --output "$output"
    python ownership-nsdi21-artifact/recovery-microbenchmark/reconstruction.py $SYSTEM_ARG --delay-ms $delay --failure --output "$output"
    python ownership-nsdi21-artifact/recovery-microbenchmark/reconstruction.py $SYSTEM_ARG --delay-ms $delay --large --output "$output"
    python ownership-nsdi21-artifact/recovery-microbenchmark/reconstruction.py $SYSTEM_ARG --delay-ms $delay --failure --large --output "$output"
done
