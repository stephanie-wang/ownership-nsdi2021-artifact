#!/bin/bash

output="output-simulated-`date`.csv"

source activate ray-0-7
ray up -y centralized.yaml
ray exec centralized.yaml "rm output.csv"
ray exec centralized.yaml "python ownership-nsdi21-artifact/model-serving/serve.py --num-nodes 4 --num-batches-per-node 4320 --centralized --output output.csv --simulate"
ray rsync-down centralized.yaml output.csv "centralized-output.csv"
mv centralized-output.csv "centralized-$output"
ray down -y centralized.yaml
conda deactivate

source activate ray-wheel

ray up -y ownership.yaml
ray exec ownership.yaml "rm output.csv"
ray exec ownership.yaml "python ownership-nsdi21-artifact/model-serving/serve.py --num-nodes 4 --num-batches-per-node 4320 --centralized --output output.csv --simulate"
ray rsync-down ownership.yaml output.csv "ownership-output.csv"
mv ownership-output.csv "ownership-$output"

ray exec ownership.yaml "rm output-no-borrow.csv"
ray exec ownership.yaml "python ownership-nsdi21-artifact/model-serving/serve.py --num-nodes 4 --num-batches-per-node 4320 --centralized --output output-no-borrow.csv --no-borrow --batch-rate 10 --simulate"
ray rsync-down ownership.yaml output-no-borrow.csv "no-borrow-output.csv"
mv no-borrow-output.csv "no-borrow-$output"

ray down -y ownership.yaml
conda deactivate
