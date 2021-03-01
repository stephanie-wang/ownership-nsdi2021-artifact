import argparse
import json
import os
import csv
import socket
import time
import datetime

import ray

import numpy as np

NODES_PER_DRIVER = 5
CHAIN_LENGTH = 10
TASKS_PER_NODE_PER_BATCH = 2000

OWNERSHIP = "ownership"
BY_VALUE = "by_value"
LEASES = "leases"
CENTRALIZED = "centralized"
SMALL_ARG = "small"
LARGE_ARG = "large"


def get_node_ids():
    my_ip = ".".join(socket.gethostname().split("-")[1:])
    node_ids = set()
    for resource in ray.available_resources():
        if "node" in resource and my_ip not in resource:
            node_ids.add(resource)
    return node_ids


def get_local_node_resource():
    my_ip = ".".join(socket.gethostname().split("-")[1:])
    addr = "node:{}".format(my_ip)
    return addr


def timeit(fn, trials=1, multiplier=1):
    start = time.time()
    for _ in range(1):
        start = time.time()
        fn()
        print("finished warmup iteration in", time.time() - start)

    stats = []
    for i in range(trials):
        start = time.time()
        fn()
        end = time.time()
        print("finished {}/{} in {}".format(i + 1, trials, end - start))
        stats.append(multiplier / (end - start))
        print("\tthroughput:", stats[-1])
    print("avg per second", round(np.mean(stats), 2), "+-",
          round(np.std(stats), 2))

    if args.output:
        file_exists = False
        try:
            os.stat(args.output)
            file_exists = True
        except:
            pass

        with open(args.output, 'a+') as csvfile:
            fieldnames = ['system', 'arg_size', 'colocated', 'num_nodes', 'throughput']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()
            for throughput in stats:
                writer.writerow({
                    'system': args.system,
                    'arg_size': args.arg_size,
                    'colocated': args.colocated,
                    'num_nodes': args.num_nodes,
                    'throughput': throughput,
                    })


@ray.remote
def f_small(*args):
    return b"hi"


@ray.remote
def f_large(*args):
    return np.zeros(1 * 1024 * 1024, dtype=np.uint8)


def do_batch(use_small, node_ids, args=None):
    if args is None:
        args = {}
        for node_id in node_ids:
            args[node_id] = None

    if use_small:
        f = f_small
    else:
        f = f_large

    results = dict()
    for node_id in node_ids:
        f_node = f.options(resources={node_id: 0.0001})

        batch = [
            f_node.remote(args[node_id])
            for _ in range(TASKS_PER_NODE_PER_BATCH)
        ]
        results[node_id] = f_node.remote(*batch)

    return results


def main(opts):
    ray.init(address="auto")

    node_ids = get_node_ids()
    assert args.num_nodes % NODES_PER_DRIVER == 0
    num_drivers = int(args.num_nodes / NODES_PER_DRIVER)
    while len(node_ids) < args.num_nodes + num_drivers:
        print("{} / {} nodes have joined, sleeping for 5s...".format(
            len(node_ids), args.num_nodes + num_drivers))
        time.sleep(5)
        node_ids = get_node_ids()

    print("All {} nodes joined.".format(len(node_ids)))
    worker_node_ids = list(node_ids)[:args.num_nodes]
    driver_node_ids = list(node_ids)[args.num_nodes:args.num_nodes +
                                     num_drivers]

    use_small = args.arg_size == "small"

    @ray.remote(num_cpus=4 if not args.colocated else 0)
    class Driver:
        def __init__(self, node_ids):
            # print("Driver starting with nodes:", node_ids)
            self.node_ids = node_ids

        def do_batch(self):
            prev = None
            for _ in range(CHAIN_LENGTH):
                prev = do_batch(use_small, self.node_ids, args=prev)

            ray.get(list(prev.values()))

        def ready(self):
            pass

    drivers = []
    for i in range(num_drivers):
        if args.colocated:
            node_id = get_local_node_resource()
        else:
            node_id = driver_node_ids[i]
        resources = {node_id: 0.001}
        worker_nodes = worker_node_ids[i * NODES_PER_DRIVER:(i + 1) *
                                       NODES_PER_DRIVER]
        drivers.append(
            Driver.options(resources=resources).remote(worker_nodes))
        ray.get(drivers[i].ready.remote())

    timeit(
        lambda: ray.get([driver.do_batch.remote() for driver in drivers]),
        multiplier=len(worker_node_ids) * TASKS_PER_NODE_PER_BATCH *
        CHAIN_LENGTH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--arg-size", type=str, required=True, help="'small' or 'large'")
    parser.add_argument(
        "--num-nodes",
        type=int,
        required=True,
        help="Number of nodes in the cluster")
    parser.add_argument(
        "--colocated",
        type=str,
        required=True,
        help="Whether to put drivers on the same node.")
    parser.add_argument("--system", type=str, required=True)
    parser.add_argument("--output", type=str, required=False)
    args = parser.parse_args()

    if args.colocated in ["True", "true"]:
        args.colocated = True
    elif args.colocated in ["False", "false"]:
        args.colocated = False
    else:
        raise ValueError("--colocated must be true or false")

    if args.system not in [OWNERSHIP, LEASES, CENTRALIZED, BY_VALUE]:
        raise ValueError("--system must be one of {}".format([OWNERSHIP, LEASES, CENTRALIZED, BY_VALUE]))
    if args.arg_size not in [SMALL_ARG, LARGE_ARG]:
        raise ValueError("--arg-size must be one of {}".format([SMALL_ARG, LARGE_ARG]))
    print(f"Running with args:", args)
    main(args)
