import numpy as np
import os

import matplotlib.pyplot as plt
from datetime import datetime


def load(filename):
    results = []
    max_frame = -1
    with open(filename, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            frame, latency = line.split(' ')
            frame = int(frame)
            latency = float(latency)
            results.append((frame, latency))
            if frame > max_frame:
                max_frame = frame
    # Remove first and last 3s of results.
    start = 90
    end = max_frame - 90
    # Subtract 1s from all results because this is the minimum with a moving
    # average of radius of 1s.
    results = [(latency - 1) * 1000 for frame, latency in results if frame > start and frame < max_frame]
    print(filename)
    print("\tmean", np.mean(results))
    print("\tp50:", np.percentile(results, 50))
    print("\tp90:", np.percentile(results, 90))
    print("\tp99:", np.percentile(results, 99))
    print("\tp100:", np.max(results))
    return results


def plot(curve, label, linestyle):
    color = next(plt.axes()._get_lines.prop_cycler)['color']
    n = np.arange(1,len(curve)+1) / np.float(len(curve))
    h, = plt.step(np.sort(curve), n, label=label, linewidth=5, linestyle=linestyle, color=color)
    plt.axvline(max(curve), linestyle=linestyle, color=color, alpha=0.5, linewidth=3)
    return h


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--num-nodes", default=60)
    parser.add_argument("--failure", action="store_true")
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--zoom", default=0, type=int)

    args = parser.parse_args()

    centralized = load(f"centralized-{args.num_nodes}.csv")
    leases = load(f"leases-{args.num_nodes}.csv")
    leases_failure = load(f"leases-{args.num_nodes}-failure.csv")
    ownership = load(f"ownership-{args.num_nodes}.csv")
    ownership_checkpoint = load(f"ownership-{args.num_nodes}-checkpoint.csv")
    ownership_failure = load(f"ownership-{args.num_nodes}-failure.csv")
    ownership_owner_failure = load(f"ownership-{args.num_nodes}-owner-failure.csv")
    ownership_owner_checkpoint_failure = load(f"ownership-{args.num_nodes}-owner-failure-checkpoint.csv")


    font = {'size': 20}
    plt.rc('font', **font)
    if args.final:
        plt.figure(figsize=(7.5, 3))

    if args.failure:
        curves = [
            (leases_failure, "L; WF", 'dashed'),
            (ownership_owner_failure, "O; OF", 'dotted'),
            (ownership_owner_checkpoint_failure, "O+CP; OF", 'solid'),
            (ownership_failure, "O; WF", 'dotted'),
            (ownership_checkpoint, "O+CP", 'solid'),
            (ownership, "O", 'dotted'),
        ]
        legend_order = [
            0, 3, 5, 2, 4, 1,
        ]
        handles = [None for _ in curves]
        labels = [None for _ in curves]
        for i, curve in enumerate(curves):
            curve, label, linestyle = curve
            curve = [p / 1000 for p in curve]
            handles[legend_order[i]] = plot(curve, label, linestyle)
            labels[legend_order[i]] = label
        plt.xlabel("Latency (s)")
        plt.ylabel("CDF")
        plt.ylim()
    else:
        plot(leases, "Leases", 'dashed')
        plot(centralized, "Centralized", 'dotted')
        plot(ownership, "Ownership", 'solid')

        plt.xlabel("Latency (ms)")
        plt.ylabel("CDF")
        

    if args.zoom:
        plt.ylim(args.zoom / 100, 1)
    else:
        plt.ylim(0, 1)

    plt.xlim(-1)
    if args.final:
        if args.failure:
            plt.legend(handles, labels, ncol=2)
            r = range(0, 21, 2)
            plt.xticks(r, [str(i) if i % 10 == 0 else '' for i in r])
        else:
            plt.ylim(0, 1)
            plt.yticks([0.00, 0.50, 1.00])
            plt.legend()
            r = range(0, 2100, 200)
            plt.xticks(r, [str(i) if i % 1000 == 0 else '' for i in r])
    else:
        plt.legend()

    plt.tight_layout()
    if args.save:
        if args.final:
            ext = "pdf"
        else:
            ext = "png"
        if args.failure:
            prefix = "latency-failure"
        else:
            prefix = "latency"
        filename = f"{prefix}-{args.num_nodes}.{ext}"
        plt.savefig(filename)
        print(f"Saved output to {filename}")
    else:
        plt.show()
