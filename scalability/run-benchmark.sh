#!/bin/bash


output="output-`date`.csv"

function run() {
    system=$1
    arg_size=$2
    colocated=$3
    for n in 100 80 60 40 20; do
	ray up -y $system.yaml
        ray exec $system.yaml "source activate tensorflow_p36 && python /home/ubuntu/ownership-nsdi21-artifact/scalability/scalability.py --system $system --arg-size $arg_size  --colocated $colocated --output output.csv --num-nodes $n"
    done
}

#source activate ray-0-7
#ray up -y local-centralized.yaml
#ray exec local-centralized.yaml "rm -f output.csv"
#run "centralized" "remote"
#run "leases" "remote"
#run "centralized" "local"
#run "leases" "local"
#ray rsync-down remote-leases.yaml output.csv tmp1.csv
#ray down -y remote-leases.yaml
#conda deactivate

#source activate ray-wheel
ray down -y ownership.yaml
ray up -y ownership.yaml
ray exec ownership.yaml "rm -f output.csv"
run "ownership" "small" "false"
run "ownership" "small" "true"
run "ownership" "large" "false"
run "ownership" "large" "true"
ray rsync-down ownership.yaml output.csv tmp2.csv
ray down -y ownership.yaml
#conda deactivate

for file in tmp1.csv tmp2.csv tmp3.csv; do
    cat $file >> "$output"
done
echo "Output written at $output"
