#!/bin/bash


output="output-`date`.csv"

function run() {
    system=$1
    mode=$2
    ray up -y $mode-$system.yaml
    ray exec $mode-$system.yaml "bash ownership-nsdi21-artifact/latency-microbenchmark/run.sh $system $mode"
}

source activate ray-0-7
ray up -y local-centralized.yaml
ray exec local-centralized.yaml "rm output.csv"
run "centralized" "remote"
run "leases" "remote"
run "centralized" "local"
run "leases" "local"
ray rsync-down remote-leases.yaml output.csv tmp1.csv
ray down -y remote-leases.yaml
conda deactivate

source activate ray-wheel
ray up -y local-ownership.yaml
ray exec local-ownership.yaml "rm output.csv"
run "ownership" "local"
run "by_value" "local"
ray rsync-down local-by_value.yaml output.csv tmp2.csv
ray down -y local-by_value.yaml
ray up -y remote-ownership.yaml
ray exec remote-ownership.yaml "rm output.csv"
run "ownership" "remote"
run "by_value" "remote"
ray rsync-down remote-by_value.yaml output.csv tmp3.csv
ray down -y remote-by_value.yaml
conda deactivate

for file in tmp1.csv tmp2.csv tmp3.csv; do
    cat $file >> "$output"
done
echo "Output written at $output"
