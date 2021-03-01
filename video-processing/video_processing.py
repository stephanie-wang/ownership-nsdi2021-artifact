import asyncio
import cv2
import os.path
import numpy as np
import time
import json
import threading
from collections import defaultdict
from ray import profiling
from ray.experimental.internal_kv import _internal_kv_put, \
    _internal_kv_get
import ray.cloudpickle as pickle
import psutil
import signal

import ray
import ray.cluster_utils


NUM_WORKERS_PER_VIDEO = 1


@ray.remote(num_cpus=0)
class Signal:
    def __init__(self):
        self.num_signals = 0

    def send(self):
        self.num_signals += 1

    def wait(self):
        return self.num_signals

    def ready(self):
        return


class Decoder:
    def __init__(self, filename, start_frame):
        self.v = cv2.VideoCapture(filename)
        self.v.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    def decode(self, frame):
        if frame != self.v.get(cv2.CAP_PROP_POS_FRAMES):
            print("next frame", frame, ", at frame", self.v.get(cv2.CAP_PROP_POS_FRAMES))
            self.v.set(cv2.CAP_PROP_POS_FRAMES, frame)
        grabbed, frame = self.v.read()
        assert grabbed
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) 
        return frame

    def ready(self):
        return

@ray.remote
def flow(prev_frame, frame, p0):
    with ray.profiling.profile("flow"):
        if p0 is None or p0.shape[0] < 100:
            p0 = cv2.goodFeaturesToTrack(prev_frame,
                                         maxCorners=200,
                                         qualityLevel=0.01,
                                         minDistance=30,
                                         blockSize=3)

        # Calculate optical flow (i.e. track feature points)
        p1, status, err = cv2.calcOpticalFlowPyrLK(prev_frame, frame, p0, None) 

        # Sanity check
        assert p1.shape == p0.shape 

        # Filter only valid points
        good_new = p1[status==1]
        good_old = p0[status==1]

        #Find transformation matrix
        m, _ = cv2.estimateAffinePartial2D(good_old, good_new)
         
        # Extract translation
        dx = m[0,2]
        dy = m[1,2]

        # Extract rotation angle
        da = np.arctan2(m[1,0], m[0,0])
         
        # Store transformation
        transform = [dx,dy,da]
        # Update features to track. 
        p0 = good_new.reshape(-1, 1, 2)

        return transform, p0


@ray.remote
def cumsum(prev, next, checkpoint_key):
    with ray.profiling.profile("cumsum"):
        sum = [i + j for i, j in zip(prev, next)]
        if checkpoint_key is not None:
            ray.experimental.internal_kv._internal_kv_put(checkpoint_key, "{} {} {}".format(*sum))
        return sum



@ray.remote
def smooth(transform, point, *window):
    with ray.profiling.profile("smooth"):
        mean = np.mean(window, axis=0)
        smoothed = mean - point + transform
        return smoothed


def fixBorder(frame):
  s = frame.shape
  # Scale the image 4% without moving the center
  T = cv2.getRotationMatrix2D((s[1]/2, s[0]/2), 0, 1.04)
  frame = cv2.warpAffine(frame, T, (s[1], s[0]))
  return frame


@ray.remote(num_cpus=0)
class Viewer:
    def __init__(self, video_pathname):
        self.video_pathname = video_pathname
        self.v = cv2.VideoCapture(video_pathname)

    def send(self, transform):
        success, frame = self.v.read() 
        assert success

        # Extract transformations from the new transformation array
        dx, dy, da = transform

        # Reconstruct transformation matrix accordingly to new values
        m = np.zeros((2,3), np.float32)
        m[0,0] = np.cos(da)
        m[0,1] = -np.sin(da)
        m[1,0] = np.sin(da)
        m[1,1] = np.cos(da)
        m[0,2] = dx
        m[1,2] = dy

        # Apply affine wrapping to the given frame
        w = int(self.v.get(cv2.CAP_PROP_FRAME_WIDTH)) 
        h = int(self.v.get(cv2.CAP_PROP_FRAME_HEIGHT))
        frame_stabilized = cv2.warpAffine(frame, m, (w,h))

        # Fix border artifacts
        frame_stabilized = fixBorder(frame_stabilized) 

        # Write the frame to the file
        frame_out = cv2.hconcat([frame, frame_stabilized])

        ## If the image is too big, resize it.
        if(frame_out.shape[1] > 1920): 
            frame_out = cv2.resize(frame_out, (frame_out.shape[1]//2, frame_out.shape[0]//2));
        
        cv2.imshow("Before and After", frame_out)
        cv2.waitKey(1)
        #out.write(frame_out)

    def ready(self):
        return


@ray.remote(num_cpus=0)
class Sink:
    def __init__(self, signal, viewer, checkpoint_interval):
        self.signal = signal
        self.num_frames_left = {}
        self.latencies = defaultdict(list)

        self.viewer = viewer
        self.last_view = None
        self.checkpoint_interval = checkpoint_interval

    def set_expected_frames(self, video_index, num_frames):
        self.num_frames_left[video_index] = num_frames
        print("Expecting", self.num_frames_left[video_index], "total frames from video", video_index)

    def send(self, video_index, frame_index, transform, timestamp):
        with ray.profiling.profile("Sink.send"):
            if frame_index < len(self.latencies[video_index]):
                return
            assert frame_index == len(self.latencies[video_index]), frame_index

            self.latencies[video_index].append(time.time() - timestamp)

            self.num_frames_left[video_index] -= 1
            if self.num_frames_left[video_index] % 100 == 0:
                print("Expecting", self.num_frames_left[video_index], "more frames from video", video_index)

            if self.num_frames_left[video_index] == 0:
                print("Video {} DONE".format(video_index))
                if self.last_view is not None:
                    ray.get(self.last_view)
                self.signal.send.remote()

            if self.viewer is not None and video_index == 0:
                self.last_view = self.viewer.send.remote(transform)

            if self.checkpoint_interval != 0 and frame_index % self.checkpoint_interval == 0:
                ray.experimental.internal_kv._internal_kv_put(video_index, frame_index, overwrite=True)

    def latencies(self):
        latencies = []
        for video in self.latencies.values():
            for i, l in enumerate(video):
                latencies.append((i, l))
        return latencies

    def ready(self):
        return


@ray.remote(num_cpus=0)
def process_chunk(video_index, video_pathname, sink, num_frames, fps, resource, v07, start_timestamp, checkpoint_interval):
    decoder_cls_args = {
            "max_reconstructions": 100,
            } if v07 else {
            "max_restarts": -1,
            "max_task_retries": -1,
            }
    decoder_cls = ray.remote(**decoder_cls_args)(Decoder)
    decoder = decoder_cls.options(
            num_cpus=1,
            resources={resource: 0.001}).remote(video_pathname, 0)

    # Check for a checkpoint.
    start_frame = 0
    radius = fps
    checkpoint_frame = ray.experimental.internal_kv._internal_kv_get(video_index)
    trajectory = []
    if checkpoint_frame is not None:
        checkpoint_frame = int(checkpoint_frame)
        print("Reloading checkpoint for video #{} at frame {}".format(video_index, checkpoint_frame))
        checkpoint_key = "{} {}".format(video_index, checkpoint_frame)
        checkpoint = ray.experimental.internal_kv._internal_kv_get(checkpoint_key)
        assert checkpoint is not None, "No checkpoint found"
        traj = [float(d) for d in checkpoint.decode('utf-8').split(' ')]
        trajectory.append(traj)

        start_frame = int(checkpoint_frame)
        print("Restarting at frame", start_frame)

    # Start at `radius` before the start frame, since we need to compute a
    # moving average.
    next_to_send = start_frame
    start_frame -= radius
    if start_frame < 0:
        padding = start_frame * -1
        start_frame = 0
    else:
        padding = 0

    frame_timestamps = []
    transforms = []
    features = None

    frame_timestamp = start_timestamp + start_frame / fps
    diff = frame_timestamp - time.time()
    if diff > 0:
        time.sleep(diff)
    frame_timestamps.append(frame_timestamp)
    prev_frame = decoder.decode.remote(start_frame)

    for i in range(start_frame, num_frames - 1):
        frame_timestamp = start_timestamp + (start_frame + i + 1) / fps
        diff = frame_timestamp - time.time()
        if diff > 0:
            time.sleep(diff)
        frame_timestamps.append(frame_timestamp)

        frame = decoder.decode.remote(start_frame + i + 1)
        flow_options = {
                "num_return_vals": 2,
                } if v07 else {
                "num_returns": 2,
                }
        flow_options["resources"] = {resource: 0.001}

        transform, features = flow.options(**flow_options).remote(prev_frame, frame, features)
        # Periodically reset the features to track for better accuracy
        # (previous points may go off frame).
        if i and i % 200 == 0:
            features = None
        prev_frame = frame
        transforms.append(transform)
        if i > 0:
            if checkpoint_interval > 0 and (i + radius) % checkpoint_interval == 0:
                # Checkpoint the cumulative sum for the first frame in the
                # window centered at the frame to checkpoint.
                checkpoint_key = "{} {}".format(video_index, i + radius)
            else:
                checkpoint_key = None
            trajectory.append(cumsum.options(resources={
                resource: 0.001
                }).remote(trajectory[-1], transform, checkpoint_key))
        else:
            # Add padding for the first few frames.
            for _ in range(padding):
                trajectory.append(transform)
            trajectory.append(transform)

        if len(trajectory) == 2 * radius + 1:
            midpoint = radius
            final_transform = smooth.options(resources={
                resource: 0.001
                }).remote(transforms.pop(0), trajectory[midpoint], *trajectory)
            trajectory.pop(0)

            sink.send.remote(video_index, next_to_send, final_transform, frame_timestamps.pop(0))
            next_to_send += 1

    while next_to_send < num_frames - 1:
        trajectory.append(trajectory[-1])
        midpoint = radius
        final_transform = smooth.options(resources={
            resource: 0.001
            }).remote(transforms.pop(0), trajectory[midpoint], *trajectory)
        trajectory.pop(0)

        final = sink.send.remote(video_index, next_to_send, final_transform,
                frame_timestamps.pop(0))
        next_to_send += 1
    return ray.get(final)


def process_videos(video_pathname, num_videos, output_filename, view,
        head_node_resource, worker_resources, owner_resources, sink_resources,
        max_frames, num_sinks, v07, checkpoint_interval, offset_seconds):
    # An actor that will get signaled once the entire job is done.
    signal = Signal.options(resources={
        head_node_resource: 0.001
        }).remote()
    ray.get(signal.ready.remote())

    if view:
        viewer = Viewer.options(resources={
            head_node_resource: 0.001
            }).remote(video_pathname)
        ray.get(viewer.ready.remote())
    else:
        viewer = None

    sinks = [Sink.options(resources={
        sink_resources[i % len(sink_resources)]: 0.001
        }).remote(signal, viewer, checkpoint_interval) for i in range(num_sinks)]
    ray.get([sink.ready.remote() for sink in sinks])

    v = cv2.VideoCapture(video_pathname)
    num_total_frames = int(min(v.get(cv2.CAP_PROP_FRAME_COUNT), max_frames))
    fps = int(v.get(cv2.CAP_PROP_FPS))
    print("Processing total frames", num_total_frames, "from video", video_pathname)
    for i in range(num_videos):
        ray.get(sinks[i % len(sinks)].set_expected_frames.remote(i, num_total_frames - 1))

    # Give the actors some time to start up.
    start_timestamp = time.time() + offset_seconds

    for sink_resource in sink_resources:
        print("Placing sink(s) on node with resource", sink_resource)
    for i in range(num_videos):
        owner_resource = owner_resources[i % len(owner_resources)]
        worker_resource = worker_resources[i % len(worker_resources)]
        print("Placing owner of video", i, "on node with resource", owner_resource)
        print("Placing workers for video", i, "on node with resource", worker_resource)
        process_chunk.options(resources={owner_resource: 0.001}).remote(
                i, video_pathname, sinks[i % len(sinks)], num_total_frames,
                fps, worker_resource, v07, start_timestamp,
                checkpoint_interval)

    # Wait for all video frames to complete. It is okay to poll this actor
    # because it is not on the critical path of video processing latency.
    ready = 0
    while ready != num_videos:
        time.sleep(1)
        ready = ray.get(signal.wait.remote())

    latencies = []
    for sink in sinks:
        latencies += ray.get(sink.latencies.remote())
    if output_filename:
        with open(output_filename, 'w') as f:
            for t, l in latencies:
                f.write("{} {}\n".format(t, l))
    else:
        for latency in latencies:
            print(latency)
    latencies = [l for _, l in latencies]
    print("Mean latency:", np.mean(latencies))
    print("Max latency:", np.max(latencies))

def kill_node(fail_at, kill_script, worker_ip):
    start = time.time()
    print("Killing", worker_ip, "after", fail_at, "s")
    cmd = "ssh -i ~/ray_bootstrap_key.pem -o StrictHostKeyChecking=no ubuntu@{} 'docker exec python3.5 bash /home/ubuntu/{}'".format(worker_ip, kill_script)
    print("Will use kill command:", cmd)
    diff = start + fail_at - time.time()
    if diff > 0:
        time.sleep(diff)
    print("KILLING WORKER")
    os.system(cmd)


def kill_local_process(fail_at, worker_failure):
    start = time.time()
    pid = None
    if worker_failure:
        pattern = "ray::Decoder"
    else:
        pattern = "ray::process_chunk"
    while pid is None:
        for p in psutil.process_iter():
            if pattern in p.name():
                pid = p.pid
                break
        time.sleep(1)

    print("Found PID to kill:", pid)
    diff = start + fail_at - time.time()
    if diff > 0:
        time.sleep(diff)
    os.kill(pid, signal.SIGKILL)


def main(args):
    num_worker_nodes = args.num_videos
    num_owner_nodes = args.num_videos // args.max_owners_per_node
    if args.num_videos % args.max_owners_per_node != 0:
        num_owner_nodes += 1
    num_sinks = args.num_videos // args.max_videos_per_sink
    if args.num_videos % args.max_videos_per_sink != 0:
        num_sinks += 1
    num_sink_nodes = num_sinks // args.max_sinks_per_node
    if num_sinks % args.max_sinks_per_node != 0:
        num_sink_nodes += 1

    num_nodes = num_worker_nodes + num_owner_nodes + num_sink_nodes

    import socket
    head_ip = socket.gethostbyname(socket.gethostname())
    head_node_resource = "node:{}".format(head_ip)

    if args.local:
        ray.init(resources={head_node_resource: 1})
    else:
        ray.init(address="auto")

    # Assign all tasks and actors resources.
    if args.local:
        worker_resources = [head_node_resource]
        owner_resources = [head_node_resource]
        sink_resources = [head_node_resource]
    else:
        print("Waiting for", num_nodes + 1, " nodes")
        nodes = [node for node in ray.nodes() if node["Alive"]]
        while len(nodes) < num_nodes + 1:
            time.sleep(10)
            print("{} nodes found, waiting for nodes to join".format(len(nodes)))
            nodes = [node for node in ray.nodes() if node["Alive"]]


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
        i = 0
        worker_resources = node_resources[:num_worker_nodes]
        i += num_worker_nodes
        owner_resources = node_resources[i:i + num_owner_nodes]
        i += num_owner_nodes
        sink_resources = node_resources[i:i + num_sink_nodes]

    # Start processing at an offset from the current time to give all processes
    # time to start up.
    offset_seconds = 5
    if args.failure or args.owner_failure:
        fail_at = offset_seconds + args.fail_at_seconds
        if args.local:
            t = threading.Thread(target=kill_local_process,
                    args=(fail_at, args.failure,))
        else:
            if args.failure:
                resource = worker_resources[0]
                kill_script = "ownership-nsdi21-artifact/video-processing/restart_decoder.sh"
            else:
                resource = owner_resources[0]
                kill_script = "ownership-nsdi21-artifact/video-processing/restart_owner.sh"
            worker_ip = None
            for node in ray.nodes():
                if node["Alive"] and resource in node["Resources"]:
                    worker_ip = node["NodeManagerAddress"]
                    break
            assert worker_ip is not None
            t = threading.Thread(target=kill_node, args=(
                fail_at, kill_script, worker_ip))
        t.start()
        process_videos(args.video_path, args.num_videos, args.output,
                args.local, head_node_resource, worker_resources,
                owner_resources, sink_resources, args.max_frames,
                num_sinks, args.v07, args.checkpoint_interval,
                offset_seconds)
        t.join()
    else:
        process_videos(args.video_path, args.num_videos, args.output,
                args.local, head_node_resource, worker_resources,
                owner_resources, sink_resources, args.max_frames,
                num_sinks, args.v07, args.checkpoint_interval,
                offset_seconds)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run the video benchmark.")

    parser.add_argument("--num-videos", required=True, type=int)
    parser.add_argument("--video-path", required=True, type=str)
    parser.add_argument("--output", type=str)
    parser.add_argument("--v07", action="store_true")
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--owner-failure", action="store_true")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--max-frames", default=600, type=int)
    # Limit the number of videos that can send to one sink.
    parser.add_argument("--max-videos-per-sink", default=9, type=int)
    parser.add_argument("--max-sinks-per-node", default=2, type=int)
    parser.add_argument("--max-owners-per-node", default=4, type=int)
    parser.add_argument("--checkpoint-interval", default=0, type=int)
    parser.add_argument("--fail-at-seconds", default=5, type=int)
    args = parser.parse_args()
    main(args)
