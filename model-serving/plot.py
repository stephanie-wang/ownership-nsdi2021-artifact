import numpy as np
import os

import matplotlib.pyplot as plt
from datetime import datetime

LINESTYLES = ['dashed', 'dotted', 'solid']
COLORS = ['tab:purple', 'tab:orange', 'tab:green']

def load(filename):
    results = []
    num_over_1000 = 0
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
    # Cut first few seconds for warmup.
    start = 1000
    end = max_frame
    results = [(latency) * 1000 for frame, latency in results if frame > start and frame < end]
    print(filename)
    print("\tmean", np.mean(results))
    print("\tp50:", np.percentile(results, 50))
    print("\tp90:", np.percentile(results, 90))
    print("\tp99:", np.percentile(results, 99))
    print("\tp100:", np.max(results))
    print("\tstd:", np.std(results))
    return results

def plot(curve, label, linestyle, color):
    n = np.arange(1,len(curve)+1) / np.float(len(curve))
    h, = plt.step(np.sort(curve), n, label=label, linewidth=5, linestyle=linestyle, color=color)
    plt.axvline(max(curve), linestyle=linestyle, color=color)
    return h


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--filename-suffix", default=None)
    parser.add_argument("--final", action="store_true")
    parser.add_argument("--zoom", default=0, type=int)

    args = parser.parse_args()
    if args.filename_suffix is None:
        args.filename_suffix = 'output-Sat 27 Feb 2021 01:12:20 PM PST.csv'

    centralized = load("centralized-" + args.filename_suffix)
    ownership = load("ownership-" + args.filename_suffix)
    no_borrow = load("no-borrow-" + args.filename_suffix)

    font = {'size': 20}
    plt.rc('font', **font)

    if args.final:
        if not args.zoom:
            plt.figure(figsize=(7.5, 3))
        else:
            plt.figure(figsize=(5, 3))

    plot(no_borrow, "Ownership -borrow", LINESTYLES[0], COLORS[0])
    plot(centralized, "Centralized", LINESTYLES[1], COLORS[1])
    plot(ownership, "Ownership", LINESTYLES[2], COLORS[2])

    plt.xlabel("Latency (ms)")
    plt.ylabel("CDF")

    if args.zoom > 0:
        plt.ylim(args.zoom / 100, 1)
        plt.yticks([i / 100 for i in range(args.zoom, 101)])
    else:
        plt.ylim(0, 1)
    plt.xlim(0)
    max_value = max(max(curve) for curve in [no_borrow, centralized, ownership])
    r = range(0, int(max_value), 100)
    if max_value > 1000:
        plt.xticks(r, [str(i) if i % 1000 == 0 else '' for i in r])
    else:
        plt.xticks(r)

    if not args.final:
        plt.legend()
    elif args.zoom == 0:
        plt.legend()
    plt.tight_layout()

    if args.save:
        if args.zoom > 0:
            name = "latency-p" + str(args.zoom)
        else:
            name = "latency"
        if args.final:
            name += ".pdf"
        else:
            name += ".png"
        plt.savefig(os.path.join(name))
    else:
        plt.show()
