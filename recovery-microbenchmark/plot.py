import csv
from collections import defaultdict
from collections import namedtuple

import matplotlib.pyplot as plt
from collections import defaultdict

Point = namedtuple("Point", ["system", "large", "delay_ms", "failure"])

def load(filename, points, keys):
    with open(filename, 'r') as f:
        r = csv.DictReader(f)
        for line in r:
            if line["system"] == "system":
                continue
            line["large"] = line["large"] == "True"
            line["failure"] = line["failure"] == "True"
            line["delay_ms"] = int(line["delay_ms"])
            line["duration"] = float(line["duration"])
            key = Point(line["system"], line["large"], line["delay_ms"], line["failure"])
            if key not in points:
                points[key] = line["duration"]
                keys.add(line["delay_ms"])
    return points

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--large", action="store_true")
    parser.add_argument("--filename", default=None)
    parser.add_argument("--final", action="store_true")

    args = parser.parse_args()
    if args.filename is None:
        args.filename = "output-Wed 24 Feb 2021 08:53:45 PM PST.csv"

    print("Loading results from file", args.filename)
    keys = set()
    points = {}
    load(args.filename, points, keys)

    if args.final:
        if args.large:
            figsize=(8.5, 4)
        else:
            figsize=(5.5, 4)
        plt.figure(figsize=figsize)

    font = {'size': 20}
    plt.rc('font', **font)

    OWNERSHIP = "ownership"
    LEASES = "leases"
    BY_VALUE = "by_value"

    LABELS = {
        OWNERSHIP: "Ownership",
        LEASES: "Leases",
        BY_VALUE: "Pass by value"
    }

    COLORS = {
        BY_VALUE: 'tab:purple',
        LEASES: 'tab:blue',
        OWNERSHIP:'tab:green',
    }

    curves = defaultdict(list)

    def plot(points, key, large):
        baseline = points[Point(system=OWNERSHIP, large=large, delay_ms=key, failure=False)]
        for system in LABELS.keys():
            for failure in [True, False]:
                k = Point(system=system, large=large, delay_ms=key, failure=failure)
                curves[(system, failure)].append(points[k] / baseline)

    keys = sorted(keys)
    for key in keys:
        plot(points, key, large=args.large)

    MARKERS = ['+', 'd', '3', '', '.', 'x']
    marker_style = dict(markersize=13, mew=3)
    i = 0
    plt.plot([-50, 1050], [1, 1], color='black', linestyle='dotted')
    for system in [BY_VALUE, LEASES, OWNERSHIP]:
        for failure in [False, True]:
            if not (system == OWNERSHIP and not failure):
                plt.plot(keys, curves[(system, failure)],
                         label="{}{}".format(LABELS[system], ";\nfailure" if failure else ""),
                         linewidth=2, marker=MARKERS[i], color=COLORS[system], **marker_style)
            print(system, "failure" if failure else "no failure")
            for key, pt in zip(keys, curves[(system, failure)]):
                print(key, pt)
            i += 1

    plt.ylabel("Relative time (log)")
    plt.xlabel("Task duration (ms)")
    if args.final:
        if args.large:
            plt.legend(bbox_to_anchor=(1.02, 1), fontsize=18)
    else:
        plt.legend(fontsize=18)

    plt.yscale('log')
    plt.ylim(0.8, 12)
    plt.tick_params(axis='y', which='minor', length=4)
    plt.yticks([1, 5, 10], [1, 5, 10])
    plt.xlim(-50, 1050)
    plt.grid(axis='y')

    plt.tight_layout()


    if args.save:
        if args.final:
            name = "reconstruction-{}.pdf".format("large" if args.large else "small")
        else:
            name = "reconstruction-{}.png".format("large" if args.large else "small")
        plt.savefig(name)
    else:
        plt.show()
