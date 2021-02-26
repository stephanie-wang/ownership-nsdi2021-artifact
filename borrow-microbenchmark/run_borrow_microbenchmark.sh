#!/bin/bash


output="borrow-`date`.csv"
echo "Writing output to $output"

function run() {
    system=$1
    output=$2
    yaml="$system.yaml"
    ray up -y $yaml
    ray exec $yaml "rm output.csv"
    for borrowers in 0 1 2 4 8 16; do
        echo "$borrowers borrowers"
        ray exec $yaml "python ownership-nsdi21-artifact/borrow-microbenchmark/borrowing.py --system $system --num-borrowers $borrowers --output output.csv"
    done
    ray rsync_down $yaml "'/home/ubuntu/output.csv'" $system-output.csv
    cat $system-output.csv >> "$output"
}

source activate ray-0-7
run "leases" "$output"
run "centralized" "$output"
conda deactivate

source activate ray-wheel
run "ownership" "$output"
conda deactivate
