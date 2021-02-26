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

NUM_TASKS = 100000

OWNERSHIP = "ownership"
CENTRALIZED = "centralized"
LEASES = "leases"


@ray.remote
def task(dep):
    return b"ok"

@ray.remote
def borrower(num_tasks, dep):
    return ray.get([task.remote(*dep) for _ in range(num_tasks)])

def main(args):
    ray.init(address="auto")

    dep = ray.put(np.zeros(1 * 1024 * 1024, dtype=np.uint8))  # 1 MiB
    # Warmup.
    ray.get([task.remote(1) for _ in range(NUM_TASKS // 10)])

    start = time.time()
    if args.num_borrowers > 0:
        ray.get([borrower.remote(NUM_TASKS // args.num_borrowers, [dep]) for _ in range(args.num_borrowers)])
    else:
        ray.get([task.remote(dep) for _ in range(NUM_TASKS)])
    runtime = time.time() - start
    print("Finished in", runtime)

    if args.output:
        file_exists = False
        try:
            os.stat(args.output)
            file_exists = True
        except:
            pass

        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'num_borrowers', 'num_tasks', 'latency']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            writer.writerow({
                'system': args.system,
                'num_tasks': NUM_TASKS,
                'num_borrowers': args.num_borrowers,
                'latency': runtime,
                })


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-borrowers", default=0, type=int)
    parser.add_argument("--system", type=str, required=True)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    if args.system not in [OWNERSHIP, CENTRALIZED, LEASES]:
        raise ValueError("--system must be one of {}".format([OWNERSHIP, CENTRALIZED, LEASES]))
    main(args)
