import asyncio
import cv2
import numpy as np
import os
import ray
from random import randrange
import sys
from tensorflow import keras
import tensorflow as tf
from time import process_time 
import time
import signal

from keras.preprocessing.image import load_img 
from keras.preprocessing.image import img_to_array 
from keras.applications.imagenet_utils import decode_predictions 
import matplotlib.pyplot as plt 
from keras.applications.resnet50 import ResNet50 
from keras.applications import resnet50
from PIL import Image


@ray.remote
def preprocess(img):
    numpy_image = img_to_array(img) 
    image_batch = np.expand_dims(numpy_image, axis = 0) 
    processed_img = resnet50.preprocess_input(image_batch.copy())
    return processed_img

# Use an actor to keep the model in GPU memory.
class Worker:
    def __init__(self, simulate):
        if simulate:
            self.model = None
        else:
            self.model = resnet50.ResNet50(input_shape=(224, 224, 3),weights = 'imagenet')
        self.latencies = []

        self.latency_sum = 0
        self.num_requests = 0

    def start(self):
        empty_img = np.empty([1, 224, 224, 3])
        if self.model:
            predictions = self.model.predict(empty_img)
        print("worker alive")

    def get_latencies(self):
        latencies = self.latencies[:]
        self.latencies = []
        return latencies

    def predict(self, request_starts, *batch):
        # Predict and return results
        start = time.time()
        img_batch = np.array(batch).reshape(len(batch),224, 224,3)
        predictions = self.model.predict(img_batch) 
        predictions = resnet50.decode_predictions(predictions, top=1)
        self.latency_sum += time.time() - start
        self.num_requests += 1

        for r in request_starts:
            self.latencies.append(time.time() - r)

        if (self.num_requests % 10):
            print("mean latency after {} requests: {}".format(
                self.num_requests, self.latency_sum / self.num_requests))

        return predictions

    def simulate_predict(self, request_starts, *batch):
        time.sleep(0.035)
        for r in request_starts:
            self.latencies.append(time.time() - r)
        return ["" for _ in batch]

    def pid(self):
        return os.getpid()


@ray.remote
class CentralizedBatcher:
    def __init__(self, batch_size, no_borrow, num_workers, worker_resource, simulate):
        self.workers = []

        num_gpus = 1
        if simulate:
            num_gpus = 0
        worker_cls = ray.remote(
                num_cpus=0,
                num_gpus=num_gpus)(Worker)

        for i in range(num_workers):
            w = worker_cls.options(resources={worker_resource: 0.001}).remote(simulate)
            ray.get(w.start.remote())
            self.workers.append(w)
        self.batch_size = batch_size
        self.queue = []
        self.predictions = []
        self.simulate = simulate

    def start(self):
        print("batcher alive")
        ray.get([worker.start.remote() for worker in self.workers])

    def request(self, img_refs, request_start):
        self.queue.append((img_refs[0], request_start))
        if len(self.queue) == self.batch_size:
            batch = []
            starts = []
            for i in range(self.batch_size):
                ref, start = self.queue.pop(0)
                batch.append(ref)
                starts.append(start)
            worker = self.workers.pop(0)
            if self.simulate:
                result_oid = worker.simulate_predict.remote(starts, *batch)
            else:
                result_oid = worker.predict.remote(starts, *batch)
            self.workers.append(worker)
            return result_oid

    def get_latencies(self):
        latencies = []
        for worker in self.workers:
            latencies += ray.get(worker.get_latencies.remote())
        return latencies

    def pid(self):
        worker_pid = ray.get(self.workers[0].pid.remote())
        return os.getpid(), worker_pid

class Batcher:
    def __init__(self, batch_size, no_borrow, num_workers, worker_resource, simulate):
        num_gpus = 1
        if simulate:
            num_gpus = 0
        worker_cls = ray.remote(
                num_cpus=0,
                num_gpus=num_gpus)(Worker)

        self.workers = []
        for i in range(num_workers):
            w = worker_cls.options(resources={worker_resource: 0.001}).remote(simulate)
            ray.get(w.start.remote())
            self.workers.append(w)
        self.batch_size = batch_size
        self.queue = asyncio.Queue(maxsize=batch_size)
        self.no_borrow = no_borrow
        self.results = []
        self.futures = []
        self.simulate = simulate

        self.worker_queue = asyncio.Queue()
        for w in self.workers:
            self.worker_queue.put_nowait(w)

    def start(self):
        print("batcher alive")
        ray.get([worker.start.remote() for worker in self.workers])

    def pid(self):
        worker_pid = ray.get(self.workers[0].pid.remote())
        return os.getpid(), worker_pid

    async def request(self, img_refs, request_start):
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        self.futures.append(fut)

        if self.no_borrow:
            await self.queue.put((img_refs, request_start))
        else:
            await self.queue.put((img_refs[0], request_start))

        if self.queue.qsize() == self.batch_size:
            batch = []
            request_starts = []
            futures = []
            for i in range(self.batch_size):
                ref, request_start = await self.queue.get()
                batch.append(ref)
                request_starts.append(request_start)
                futures.append(self.futures.pop(0))
            worker = await self.worker_queue.get()
            if self.simulate:
                predictions = await worker.simulate_predict.remote(request_starts, *batch)
            else:
                predictions = await worker.predict.remote(request_starts, *batch)
            self.worker_queue.put_nowait(worker)
            for pred, future in zip(predictions, futures):
                future.set_result(pred)

        return await fut

    def get_latencies(self):
        latencies = []
        for worker in self.workers:
            latencies += ray.get(worker.get_latencies.remote())
        return latencies


# Generate Batcher requests
@ray.remote
class Client:
    # One batcher per client
    def __init__(self, batcher, no_borrow, worker_resource, batch_rate, batch_size):
        self.batcher = batcher
        self.no_borrow = no_borrow
        self.worker_resource = worker_resource
        self.img = load_img('/home/ubuntu/ownership-nsdi21-artifact/model-serving/img.jpg', target_size=(224, 224))
        self.request_interval = 1.0 / (batch_rate * batch_size)
        self.batch_size = batch_size

    def start(self):
        print("client alive")
        ray.get(self.batcher.start.remote())

    def run_concurrent(self, num_batches, centralized, backpressure):
        img = ray.put(self.img)

        results = []
        start = time.time()
        next_request_at = start
        for i in range(num_batches * self.batch_size):
            diff = next_request_at - time.time()
            if diff > 0:
                time.sleep(diff)

            request_start = time.time()
            img_ref = preprocess.options(resources={self.worker_resource: 0.001}).remote(img)
            if self.no_borrow:
                ref = self.batcher.request.remote(img_ref, request_start)
            else:
                ref = self.batcher.request.remote([img_ref], request_start)
            results.append(ref)

            if len(results) >= self.batch_size * 4:
                num_results = len(results) // 2
                # Turn off backpressure by default so that we generate the same
                # request load for all systems.
                if backpressure:
                    self._flush(results[num_results:], centralized)
                results = results[num_results:]

            next_request_at += self.request_interval

        print("Finished submitting", num_batches, "batches in", time.time() - start)
        self._flush(results, centralized)
        return

    def _flush(self, results, centralized):
        if centralized:
            result_oids = ray.get(results)
            result_oids = [oid for oid in result_oids if oid is not None]
            ray.get(result_oids)
        else:
            ray.get(results)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-nodes", required=True, type=int)
    parser.add_argument("--num-batches-per-node", required=True, type=int)
    parser.add_argument("--batch-size", default=16, type=int)
    parser.add_argument("--num-clients-per-node", default=2, type=int)
    parser.add_argument("--num-batchers-per-node", default=1, type=int)
    parser.add_argument("--num-workers-per-node", default=8, type=int)
    parser.add_argument("--no-borrow", action="store_true")
    # How many batches per second each client should submit.
    parser.add_argument("--batch-rate", default=20, type=int)
    parser.add_argument("--output", default=None, type=str)
    parser.add_argument("--centralized", action="store_true")
    parser.add_argument("--simulate", action="store_true")
    parser.add_argument("--backpressure", action="store_true")
    args = parser.parse_args()

    ray.init(address="auto", log_to_driver=False)

    nodes = [node for node in ray.nodes() if node["Alive"]]
    while len(nodes) < args.num_nodes + 1:
        time.sleep(1)
        print("{} nodes found, waiting for nodes to join".format(len(nodes)))
        nodes = [node for node in ray.nodes() if node["Alive"]]

    import socket
    ip_addr = socket.gethostbyname(socket.gethostname())
    head_node_resource = "node:{}".format(ip_addr)

    node_resources = []
    for node in nodes:
        if not node["Alive"]:
            continue
        if head_node_resource in node["Resources"]:
            continue
        for r in node["Resources"]:
            if "node" in r:
                node_resources.append(r)

    print("All nodes joined")
    node_resources = node_resources[:args.num_nodes]
    for r in node_resources:
        print(r)

    # Assign worker nodes
    if args.centralized:
        batcher_cls = CentralizedBatcher
    else:
        batcher_cls = ray.remote(Batcher)
    batchers = []
    for i in range(args.num_batchers_per_node * args.num_nodes):
        resource = node_resources[i % len(node_resources)]
        batcher = batcher_cls.options(resources={resource: 0.001}).remote(
            args.batch_size, args.no_borrow,
            (args.num_workers_per_node // args.num_batchers_per_node), resource,
            args.simulate)
        batchers.append(batcher)
    clients = []
    for i in range(args.num_clients_per_node * args.num_nodes):
        resource = node_resources[i % len(node_resources)]
        client = Client.options(resources={resource: 0.001}).remote(
            batchers[i % len(batchers)], args.no_borrow, resource,
            args.batch_rate, args.batch_size)
        clients.append(client)

    print("Warmup for 10s")
    for _ in range(10):
        ray.get([client.run_concurrent.remote(args.batch_rate, args.centralized, args.backpressure) for client in clients])
        latencies = ray.get([b.get_latencies.remote() for b in batchers])
        print(latencies[0][-1])
    print("Done with warmup")

    # Measure throughput
    tstart = time.time()
    results = []
    num_batches = args.num_batches_per_node // args.num_clients_per_node
    for client in clients:
        results.append(client.run_concurrent.remote(num_batches, args.centralized, args.backpressure))
    ray.get(results)

    results = ray.get([b.get_latencies.remote() for b in batchers])
    if args.output is not None:
        for result in results:
            with open(args.output, 'a+') as f:
                for i, r in enumerate(result):
                    f.write("{} {}\n".format(i, r))

    results = [r for result in results for r in result]
    print("mean", np.mean(results))
    print("max", np.max(results))
    print("std", np.std(results))
    #tstop = process_time() 
    tstop = time.time()
    print("time: ", tstop-tstart)
    print("throughput: ", args.num_batches_per_node * args.num_nodes  * args.batch_size / (tstop-tstart))
