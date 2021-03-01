#!/bin/bash


system=$1
num_videos=$2
output=$3
extra_args=$4

if [ -z `ls "$system.yaml.template"` ]; then
    echo "Usage: ./run.sh <centralized|leases|ownership> <number of concurrent videos> <output filename>"
    exit -1
fi

if [ -z $num_videos ]; then
    echo "Usage: ./run.sh <centralized|leases|ownership> <number of concurrent videos> <output filename>"
    exit -1
fi


let num_owner_nodes=($num_videos / 4)
if [ $(( $num_videos % 4 )) -ne 0 ]; then
    (( num_owner_nodes += 1 ))
fi
let num_sink_nodes=($num_videos / 18 )
if [ $(( $num_videos % 18 )) -ne 0 ]; then
    (( num_sink_nodes += 1 ))
fi
let num_nodes=( $num_videos + $num_owner_nodes + $num_sink_nodes )
echo "Requesting $num_nodes nodes..."

if [ $system == "leases" ] || [ $system == "centralized" ]; then
    source activate ray-0-7
    v07="--v07"
else
    source activate ray-wheel
    v07=""
fi

sed "s/NUM_NODES/$num_nodes/g" $system.yaml.template > $system.yaml
ray up -y $system.yaml

if [ $system == "leases" ] || [ $system == "centralized" ]; then
    ray exec $system.yaml --docker ". /root/.bashrc && cd /home/ubuntu && python ownership-nsdi21-artifact/video-processing/video_processing.py --num-videos $num_videos --video-path /home/ubuntu/ownership-nsdi21-artifact/video-processing/husky.mp4 --max-frames 1800 $v07 $extra_args --output='$output'"
    ray exec $system.yaml "docker cp python3.5:/home/ubuntu/$output $output"
    ray rsync-down $system.yaml "$output" .
else
    ray exec $system.yaml ". /root/.bashrc && cd /home/ubuntu && python ownership-nsdi21-artifact/video-processing/video_processing.py --num-videos $num_videos --video-path /home/ubuntu/ownership-nsdi21-artifact/video-processing/husky.mp4 --max-frames 1800 $v07 $extra_args --output='$output'"
    ray rsync-down $system.yaml "/home/ubuntu/$output" .
fi
