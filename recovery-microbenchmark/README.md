1. (20 min) From this directory, run the benchmark.
```
$ bash run-benchmark.sh
```
The output will be saved in a file structured with `output-<date>.csv`. There is an example in this directory with the name `output-Wed 24 Feb 2021 08:53:45 PM PST.csv`.

2. (1 min) Plot the results. For example:
```
python plot.py
python plot.py --large
```
These use the committed results as the input. Replace the results by passing in the `--filename=<output CSV>` parameter.
You should see results that look something like the following:

![small](reconstruction-small.png)
![large](reconstruction-large.png)

The plotting script will also print out the raw values (run time relative to ownership without failure) that are plotted.
