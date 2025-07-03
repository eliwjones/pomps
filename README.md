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

There are no external dependencies, this is tested on Windows, Mac OS and Linux with Python 3.9 and PyPy 3.9.

```
$ git clone git@github.com:eliwjones/pomps.git
$ cd pomps
$ python test_pomps.py  # or python example.py if you have ~42 minutes to spare (~23 minutes with pypy)
```

If you choose to run example.py, after it is complete you can view the final data like so:
```
$ cat data/example/20230118-120000-000000/name_data/merged.jsonl | grep '"name": "Marlon Brando"' | python -m json.tool
{
    "imdb_nconst": "nm0000008",
    "name": "Marlon Brando",
    "birth_year": "1924",
    "death_year": "2004",
    "professions": [
        "actor",
        "director",
        "writer"
    ],
    "popular_titles": [
        {
            "imdb_tconst": "tt0047296",
            "title": "On the Waterfront",
            "year": "1954",
            "category": "actor"
        },
        {
            "imdb_tconst": "tt0068646",
            "title": "The Godfather",
            "year": "1972",
            "category": "actor"
        },
        {
            "imdb_tconst": "tt0070849",
            "title": "Last Tango in Paris",
            "year": "1972",
            "category": "actor"
        },
        {
            "imdb_tconst": "tt0078788",
            "title": "Apocalypse Now",
            "year": "1979",
            "category": "actor"
        }
    ]
}
```

## Features

Basic resumeability is built in.  If you are in the middle of a large load, merge of multiple datasets and a bug breaks a transform, load or group func, you can fix your bug and just re-run the code using the same namespace, execution_date.  It will pick up where it last left off.

Checks like [this](https://github.com/eliwjones/pomps/blob/8071727d71408182c60cbecb4a853c32e18039b2/pomps.py#L17-L20) or [this](https://github.com/eliwjones/pomps/blob/8071727d71408182c60cbecb4a853c32e18039b2/pomps.py#L69-L71) are what enable this behavior.

## TODO

The general aim is to keep this as stupidly simple as possible while clearly showing how to transform, group and merge data.

With that said, we are leaving a lot of CPU on the table when grouping and sorting the buckets, ~~so I may add a multiprocessing Pool somewhere in~~ [here](https://github.com/eliwjones/pomps/blob/bbcf534282152a2e000c1003ecf79f4e08794fb2/pomps.py#L129-L158).  Tests indicated that the performance bump was less than 30%, so it is not worth the complexity tradeoff.  We'd prefer to just rewrite to golang if speed and concurrency are desired.
