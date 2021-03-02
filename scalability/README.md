1. (TODO min) From this directory, run the benchmark.
```
$ bash run-benchmark.sh
```
The output will be saved in a file structured with `output-<date>.csv`. There is an example in this directory with the name `TODO`.

2. (1 min) Plot the results. For example:
```
python plot.py
```
These use the committed results as the input. Replace the results by passing in the `--filename=<output CSV>` parameter.
You should see results that look something like the following:

![latency](latency.png)

The plotting script will also print out the raw values (throughput tasks/s) that are plotted.
