#!/bin/bash


system=$1

for n in range 20 40 60 80 100; do
    python ownership-nsdi21-artifact/scalability/scalability.py --num-nodes $n --system ownership --multi-node true --arg-size small
done
