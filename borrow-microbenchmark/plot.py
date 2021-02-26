from collections import defaultdict
import csv

import matplotlib.pyplot as plt

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--filename", default=None)
    parser.add_argument("--final", action="store_true")

    args = parser.parse_args()
    if args.filename is None:
        args.filename = 'borrow-Thu 25 Feb 2021 10:21:11 PM PST.csv'

    max_num_borrowers = 8
    results = defaultdict(list)
    with open(args.filename, 'r') as f:
        for line in csv.DictReader(f):
            system = line['system']
            if not system in ["ownership", "leases", "centralized"]:
                continue
            num_borrowers = int(line['num_borrowers'])
            if num_borrowers <= max_num_borrowers:
                num_tasks = int(line['num_tasks'])
                runtime = float(line['latency'])
                tput = num_tasks / runtime
                results[system].append((num_borrowers, tput))


    font = {'size': 20}
    plt.rc('font', **font)
    if args.final:
        plt.figure(figsize=(4.5, 3.4))


    LEASES = "leases"
    CENTRALIZED = "centralized"
    OWNERSHIP = "ownership"

    COLORS = ['tab:blue', 'tab:orange', 'tab:green']
    MARKERS = ['x', '1', '+']
    marker_style = dict(markersize=12, mew=3)

    for i, system in enumerate([LEASES, CENTRALIZED, OWNERSHIP]):
        curve = results[system]
        print(system)
        for num_borrowers, tput in curve:
            print(num_borrowers, "borrowers, ", tput, "tasks/s")
        x, y = zip(*curve)
        plt.plot(x, y, linewidth=3, marker=MARKERS[i], color=COLORS[i], **marker_style, label=system)
    plt.xticks(x)
    if not args.final:
        plt.legend()
    plt.ylabel("Throughput\n(tasks/s)")
    plt.yticks([0, 10000, 20000, 30000], ["0", "10k", "20k", "30k"])
    plt.xlabel("# nested tasks")
    plt.grid(axis='y')
    plt.tight_layout()

    if args.save:
        if args.final:
            plt.savefig("borrow.pdf")
        else:
            plt.savefig("borrow.png")
    else:
        plt.show()
