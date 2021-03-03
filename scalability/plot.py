import argparse
from collections import namedtuple, defaultdict
import csv

import matplotlib.pyplot as plt

parser = argparse.ArgumentParser()
parser.add_argument("--filename", default='output-Wed Mar  3 05:44:38 UTC 2021.csv')
args = parser.parse_args()

Key = namedtuple('Key', ['system', 'small', 'colocated'])
OWNERSHIP = "ownership"
CENTRALIZED = "centralized"
LEASES = "leases"
BY_VALUE = "by_value"

results = defaultdict(list)

with open(args.filename, 'r') as f:
    r = csv.DictReader(f)
    for line in r:
        system = line["system"]
        if system not in [OWNERSHIP, CENTRALIZED, LEASES, BY_VALUE]:
            continue
        key = Key(system=system, small=line['arg_size'] == 'small', colocated=line['colocated'] == 'True')
        num_nodes = int(line['num_nodes'])
        throughput = float(line['throughput'])
        print(key, num_nodes, throughput)
        results[key].append((num_nodes, throughput))



SAVE = True
font = {'size': 20}
plt.rc('font', **font)

COLORS = ['tab:purple', 'tab:blue', 'tab:orange', 'tab:green']
MARKERS = ['^', 'x', '1', '+']
LABELS = {
    OWNERSHIP: "Ownership",
    CENTRALIZED: "Centralized",
    LEASES: "Leases",
    BY_VALUE: "Pass by\nvalue",
}
marker_style = dict(markersize=12, mew=3)

for small in [True, False]:
    for colocated in [True, False]:
        plt.figure(figsize=(5, 3))
        i = 0
        for system in [BY_VALUE, LEASES, CENTRALIZED, OWNERSHIP]:
            key = Key(system, small, colocated)
            if key in results:
                points = results[key]
                x, y = zip(*points)
                plt.yticks([0, 50000, 100000, 150000], ["0", "50k", "100k", "150k"])
                plt.plot(x, y, label=LABELS[system], linewidth=2, marker=MARKERS[i], color=COLORS[i], **marker_style)
                plt.ylim(0, 170000)
                plt.xticks(range(20, 101, 20))

            i += 1
        plt.xlabel("Worker nodes")
        plt.ylabel("Throughput\n(tasks/s)")
        plt.grid(axis='y')
        plt.tight_layout()
        if SAVE:
            plt.savefig('scalability-{}-{}.pdf'.format("small" if small else "large", "colocated" if colocated else "spread"))
            plt.clf()
        else:
            plt.show()
if SAVE:
    plt.figure(figsize=(5, 4))
    for i, system in enumerate([BY_VALUE, LEASES, CENTRALIZED, OWNERSHIP]):
        plt.plot([], [], label=LABELS[system], marker=MARKERS[i], color=COLORS[i], **marker_style)
    plt.legend(framealpha=1)
    plt.tight_layout()
    plt.savefig("scalability-legend.pdf")
