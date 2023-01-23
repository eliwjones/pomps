# Pomps

Poor Man's PySpark

## Description

This is meant to give a simple example of the apparatus needed to load, transform and merge datasets of indeterminate size without the use of a more complex framework like Spark.

1. [load_and_transform_source_data](https://github.com/eliwjones/pomps/blob/37b96e23729170e6d896b0ec9732c7d15688e765/pomps.py#L11) - A `load_func()` is passed that accepts a `filepath` arg.  It is expected this results in a jsonl file at the filepath specified.
A `transform_func()` tells us how to shape the data.

2. [group_data](https://github.com/eliwjones/pomps/blob/37b96e23729170e6d896b0ec9732c7d15688e765/pomps.py#L48) - A `group_key_func()` is passed telling us how to group the data.  With this, a sort-merge index is created.

3. [merge_data_sources](https://github.com/eliwjones/pomps/blob/37b96e23729170e6d896b0ec9732c7d15688e765/pomps.py#L241) - Given two data sets grouped by the `group_data()` function, we pass a `merge_func()` that is used to combine this data together into a new dataset.

## Example

A toy example can be seen in the `test_pomps.py` file, [test_merge_data_sources](https://github.com/eliwjones/pomps/blob/37b96e23729170e6d896b0ec9732c7d15688e765/test_pomps.py#L139) - This creates some jsonl files on the fly, groups, merges them together and tests the result looks as expected.

The [example.py](https://github.com/eliwjones/pomps/blob/master/example.py) file shows a more realistic example that merges together some of the IMDB data files found [here](https://datasets.imdbws.com/).  Be warned, this IMDB data appears to have undependable `nconst` and `tconst` ids (various runs can load title data that points to an `nconst` that is not found in the loaded name data.)  But, it proves out the internal workings of Pomps well enough.

## Quickstart

There are no external dependencies, this is tested on Windows and Linux with Python 3.9 but should work just fine on OS X as well.

```
$ git clone git@github.com:eliwjones/pomps.git
$ cd pomps
$ python test_pomps.py  # or python example.py if you have ~45 minutes to spare.
```

## TODO

The general aim is to keep this as stupidly simple as possible while clearly showing how to transform, group and merge data.

With that said, we are leaving a lot of CPU on the table when grouping and sorting the buckets, so I may add a multiprocessing Pool somewhere in [here](https://github.com/eliwjones/pomps/blob/bbcf534282152a2e000c1003ecf79f4e08794fb2/pomps.py#L129-L158)
