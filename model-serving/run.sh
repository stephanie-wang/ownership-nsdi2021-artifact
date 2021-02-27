#!/bin/bash

output="output-`date`.csv"

source activate ray-0-7
ray up -y centralized-gpu.yaml
ray exec centralized-gpu.yaml "rm output.csv"
ray exec centralized-gpu.yaml "python ownership-nsdi21-artifact/model-serving/serve.py --num-nodes 4 --num-batches-per-node 4320 --centralized --output output.csv"
ray rsync-down centralized-gpu.yaml output.csv "centralized-output.csv"
mv centralized-output.csv "centralized-$output"
ray down -y centralized-gpu.yaml
conda deactivate

source activate ray-wheel

ray up -y ownership-gpu.yaml
ray exec ownership-gpu.yaml "rm output.csv"
ray exec ownership-gpu.yaml "python ownership-nsdi21-artifact/model-serving/serve.py --num-nodes 4 --num-batches-per-node 4320 --centralized --output output.csv"
ray rsync-down ownership-gpu.yaml output.csv "ownership-output.csv"
mv ownership-output.csv "ownership-$output"

ray exec ownership-gpu.yaml "rm output.csv"
ray exec ownership-gpu.yaml "python ownership-nsdi21-artifact/model-serving/serve.py --num-nodes 4 --num-batches-per-node 4320 --centralized --output output.csv --no-borrow --batch-rate 10"
ray rsync-down ownership-gpu.yaml output.csv "no-borrow-output.csv"
mv no-borrow-output.csv "no-borrow-$output"

ray down -y ownership-gpu.yaml
conda deactivate
