1. From this directory, run the benchmark.
```
$ bash run-benchmark.sh
```

The output will be saved in a file structured with `output-<date>.csv`. There is an example in this directory with the name `output-Wed Mar  3 05:44:38 UTC 2021.csv`.

NOTE: While this runs, you may seem some of the following error messages:
```
(pid=raylet, ip=172.31.50.251) F0302 06:10:34.762667  5395 grpc_server.cc:37]  Check failed: PortNotInUse(port_) Port 8076 specified by caller already in use. Try passing node_manager_port=... into ray.init() to pick a specific port
```
This is expected; it happens sometimes on older Ray versions when the Ray runtime is restarted (between experiments).

2. Plot the results. For example:
```
python plot.py
```
These use the committed results as the input. Replace the results by passing in the `--filename=<output CSV>` parameter.
You should see results that look something like the following:

![latency](latency.png)

The plotting script will also print out the raw values (throughput tasks/s) that are plotted.
