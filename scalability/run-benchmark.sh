#!/bin/bash


output="output-`date`.csv"

function run() {
    system=$1
    arg_size=$2
    colocated=$3
    for n in 100 80 60 40 20; do
        ray exec $system.yaml "source activate tensorflow_p36 && python /home/ubuntu/ownership-nsdi21-artifact/scalability/scalability.py --system $system --arg-size $arg_size  --colocated $colocated --output output.csv --num-nodes $n"
    done
}

source activate ray-0-7

ray up -y centralized.yaml
ray exec centralized.yaml "rm -f output.csv"
run "centralized" "small" "false"
ray up -y centralized.yaml
run "centralized" "small" "true"
ray up -y centralized.yaml
run "centralized" "large" "false"
ray up -y centralized.yaml
run "centralized" "large" "true"
ray rsync-down centralized.yaml output.csv centralized.csv

ray up -y leases.yaml
ray exec leases.yaml "rm -f output.csv"
run "leases" "small" "false"
ray up -y leases.yaml
run "leases" "small" "true"
ray up -y leases.yaml
run "leases" "large" "false"
ray up -y leases.yaml
run "leases" "large" "true"
ray rsync-down leases.yaml output.csv leases.csv
ray down -y leases.yaml

conda deactivate

source activate ray-wheel

ray up -y ownership.yaml
ray exec ownership.yaml "rm -f output.csv"
run "ownership" "small" "false"
ray up -y ownership.yaml
run "ownership" "small" "true"
ray up -y ownership.yaml
run "ownership" "large" "false"
ray up -y ownership.yaml
run "ownership" "large" "true"
ray rsync-down ownership.yaml output.csv ownership.csv

ray up -y by_value.yaml
ray exec by_value.yaml "rm -f output.csv"
run "by_value" "small" "false"
ray up -y by_value.yaml
run "by_value" "small" "true"
ray rsync-down by_value.yaml output.csv by_value.csv
ray down -y by_value.yaml

conda deactivate

for file in centralized.csv leases.csv ownership.csv by_value.csv; do
    cat $file >> "$output"
done
echo "Output written at $output"
