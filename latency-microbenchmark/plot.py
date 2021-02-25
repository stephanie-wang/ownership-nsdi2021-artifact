import csv
from collections import defaultdict
from collections import namedtuple

import matplotlib.pyplot as plt
import numpy as np

Key = namedtuple("Key", ["local", "actors"])

OWNERSHIP = "ownership"
CENTRALIZED = "centralized"
LEASES = "leases"
BY_VALUE = "by_value"

LABELS = {
    OWNERSHIP: "Ownership",
    CENTRALIZED: "Centralized",
    LEASES: "Leases",
    BY_VALUE: "Pass by\nvalue",
}

COLORS = {
    BY_VALUE: 'tab:purple',
    LEASES: 'tab:blue',
    CENTRALIZED: 'tab:orange',
    OWNERSHIP:'tab:green',
}

def load(filename, results):
    with open(filename, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            system = row["system"]
            if system not in [OWNERSHIP, CENTRALIZED, LEASES, BY_VALUE]:
                continue
            if row["pipelined"] == "True":
                key = Key(
                    row["local"] == "True",
                    row["actors"] == "True")
                results[system][key].append(float(row["latency"]))
            
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--filename", default=None)
    parser.add_argument("--final", action="store_true")

    args = parser.parse_args()
    if args.filename is None:
        args.filename = "output-Wed 24 Feb 2021 10:47:38 PM PST.csv"

    results = {
        OWNERSHIP: defaultdict(list),
        CENTRALIZED: defaultdict(list),
        LEASES: defaultdict(list),
        BY_VALUE: defaultdict(list),
    }
    load(args.filename, results)

    labels = []
    curves = defaultdict(list)
    for local in [True, False]:
        for actors in [True, False]:
            for key in [BY_VALUE, CENTRALIZED, LEASES, OWNERSHIP]:
                r = results[key][(local, actors)]
                curves[key].append((np.mean(r) * 1000, np.std(r) * 1000))
            labels.append("{}\n{}".format("local" if local else "remote",
                                            "actors" if actors else "tasks"))

    plt.figure(figsize=(9.75, 4))
    font = {'size': 20}
    plt.rc('font', **font)

    width = 0.2
    x = np.arange(len(labels))
    for i, curve in enumerate([BY_VALUE, CENTRALIZED, LEASES, OWNERSHIP]):
        means, stds = zip(*curves[curve])
        plt.bar(x + i * width - 1.5 * width, means, width, label=LABELS[curve], color=COLORS[curve])
        plt.errorbar(x + i * width - 1.5 * width, means, yerr=stds, fmt='none', ecolor='black', capsize=6, linewidth=3)
        print(curve)
        for label, mean in zip(labels, means):
            print(label.replace('\n', ' '), mean)

    plt.ylabel("Latency (ms)")
    plt.legend(ncol=2)
    plt.xticks(x, labels)
    plt.yticks(range(0, 5, 1))
    plt.axes().set_axisbelow(True)
    plt.grid(axis='y')
    plt.tight_layout()
    plt.xlim(-0.5, 3.5)

    if args.save:
        if args.final:
            plt.savefig("latency.pdf")
        else:
            plt.savefig("latency.png")
    else:
        plt.show()
