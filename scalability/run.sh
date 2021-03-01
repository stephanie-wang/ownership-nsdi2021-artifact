#!/bin/bash
system=$1
arg_size=$2
colocated=$3

for n in 100 80 60 40 20; do
    python /home/ubuntu/ownership-nsdi21-artifact/scalability/run.sh --system $system --colocated $colocated --arg-size $arg_size --output output.csv --num-nodes $n 
done
