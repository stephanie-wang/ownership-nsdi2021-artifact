#!/bin/bash


output="output-`date`.csv"

source activate ray-0-7
ray up -y reconstruction-leases.yaml
ray exec reconstruction-leases.yaml "rm output.csv"
ray exec reconstruction-leases.yaml "bash ownership-nsdi21-artifact/recovery-microbenchmark/run.sh output.csv --v07"
ray rsync-down reconstruction-leases.yaml output.csv leases-output.csv
conda deactivate

source activate ray-wheel
ray up -y reconstruction-by_value.yaml
ray exec reconstruction-leases.yaml "rm output.csv"
ray exec reconstruction-by_value.yaml "bash ownership-nsdi21-artifact/recovery-microbenchmark/run.sh output.csv --by-value"
ray rsync-down reconstruction-by_value.yaml output.csv by_value-output.csv

ray up -y reconstruction-ownership.yaml
ray exec reconstruction-leases.yaml "rm output.csv"
ray exec reconstruction-ownership.yaml "bash ownership-nsdi21-artifact/recovery-microbenchmark/run.sh output.csv"
ray rsync-down reconstruction-ownership.yaml output.csv ownership-output.csv
conda deactivate

for file in leases-output.csv by_value-output.csv ownership-output.csv; do
    cat $file >> "$output"
    rm $file
done
echo "Output written at $output"
