import cv2
import time
import json
import numpy as np
import threading
from collections import defaultdict
from ray import profiling
import os
import csv

import ray
import ray.cluster_utils

JOB_TIME = 10


@ray.remote
def chain(i, dep, delay_ms):
    time.sleep(delay_ms / 1000.0)
    return dep

@ray.remote
def small_dep():
    return 1

@ray.remote
def large_dep():
    return np.zeros(10 * 1024 * 1024, dtype=np.uint8)  # 10 MiB

def run(delay_ms, large, num_rounds):
    f = small_dep
    if large:
        f = large_dep
    start = time.time()
    dep = f.remote()
    for i in range(num_rounds):
        dep = chain.remote(i, dep, delay_ms)
    ray.get(dep)
    return time.time() - start

def main(args):
    if args.v07:
        ray.init(address="auto")
    else:
        ray.init(address="auto")

    num_nodes = 1
    nodes = ray.nodes()
    while len(nodes) < num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = ray.nodes()
    print("All nodes joined")

    num_rounds = int(JOB_TIME / (args.delay_ms / 1000))
    print("Running", num_rounds, "rounds of", args.delay_ms, "ms each")

    for node in nodes:
        if node["Alive"]:
            if "CPU" in node["Resources"]:
                worker_ip = node["NodeManagerAddress"]
            else:
                head_ip = node["NodeManagerAddress"]
    if args.failure:
        sleep = JOB_TIME / 2
        print("Killing", worker_ip, "after {}s".format(sleep))
        def kill():
            if args.v07:
                script = "ownership-nsdi21-artifact/restart_0_7.sh"
            elif args.by_value:
                script = "ownership-nsdi21-artifact/restart_by_value.sh"
            else:
                script = "ownership-nsdi21-artifact/restart.sh"
            cmd = 'ssh -i ~/ray_bootstrap_key.pem -o StrictHostKeyChecking=no {} "bash -s" -- < {} {}'.format(worker_ip, script, head_ip)
            print(cmd)
            time.sleep(sleep)
            os.system(cmd)
            for node in ray.nodes():
                if "CPU" in node["Resources"]:
                    print("Restarted node at {}:{}".format(node["NodeManagerAddress"], node["NodeManagerPort"]))
                    break

        t = threading.Thread(target=kill)
        t.start()
        duration = run(args.delay_ms, args.large, num_rounds)
        t.join()
    else:
        duration = run(args.delay_ms, args.large, num_rounds)

    print("Task delay", args.delay_ms, "ms. Duration", duration)

    if args.output:
        file_exists = False
        try:
            os.stat(args.output)
            file_exists = True
        except:
            pass

        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'large', 'delay_ms', 'duration', 'failure']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            system = "ownership"
            if args.v07:
                system = "leases"
            elif args.by_value:
                system = "by_value"

            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'system': system,
                'large': args.large,
                'delay_ms': args.delay_ms,
                'duration': duration,
                'failure': args.failure,
                })

    if args.timeline:
        ray.timeline(filename=args.timeline)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--large", action="store_true")
    parser.add_argument("--timeline", type=str, default=None)
    parser.add_argument("--v07", action="store_true")
    parser.add_argument("--by-value", action="store_true")
    parser.add_argument("--delay-ms", required=True, type=int)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    main(args)
