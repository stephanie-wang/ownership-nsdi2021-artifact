#!/bin/bash


num_nodes=$1

bash run.sh leases $num_nodes leases-$num_nodes.csv
bash run.sh leases $num_nodes leases-$num_nodes-failure.csv "--failure"

bash run.sh centralized $num_nodes centralized-$num_nodes.csv
ray down -y centralized.yaml

bash run.sh ownership $num_nodes ownership-$num_nodes.csv
bash run.sh ownership $num_nodes ownership-$num_nodes-checkpoint.csv '--checkpoint-interval 30'
bash run.sh ownership $num_nodes ownership-$num_nodes-failure.csv '--failure --fail-at-seconds 10'
bash run.sh ownership $num_nodes ownership-$num_nodes-owner-failure.csv '--owner-failure --fail-at-seconds 10'
bash run.sh ownership $num_nodes ownership-$num_nodes-owner-failure-checkpoint.csv '--owner-failure --checkpoint-interval 30 --fail-at-seconds 10'
ray down -y ownership.yaml

output="output-$num_nodes-nodes-`date`.zip"
echo "zip '$output' centralized-$num_nodes.csv leases-$num_nodes.csv leases-$num_nodes-failure.csv ownership-$num_nodes-failure.csv ownership-$num_nodes.csv ownership-$num_nodes-checkpoint.csv ownership-$num_nodes-owner-failure-checkpoint.csv ownership-$num_nodes-owner-failure.csv"
