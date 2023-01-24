import csv
import gzip
import json
import time
import urllib.request

from datetime import datetime

import pomps

DATA_DIR = './data'
ENV = 'example'


def load_imdb_data_func(url):
    desired_fields = {
        'birthYear',
        'category',
        'deathYear',
        'endYear',
        'knownForTitles',
        'nconst',
        'primaryName',
        'primaryProfession',
        'primaryTitle',
        'startYear',
        'tconst',
    }

    def func(filepath):
        with urllib.request.urlopen(url) as r:
            with gzip.open(r, mode='rt', encoding='utf-8') as gzip_f, open(filepath, 'w', encoding='utf-8') as f:
                reader = csv.DictReader(gzip_f, delimiter='\t')
                counter = 0
                for doc in reader:
                    new_doc = {key: val for key, val in doc.items() if key in desired_fields and val != '\\N'}
                    f.write(json.dumps(new_doc) + '\n')

                    counter += 1
                    if not counter % pomps.DEBUG_MODULUS:
                        print(f"[load_imdb_data_func] url: {url}, writing line {counter}")

    return func


def transform_title_principals(doc):
    field_map = {'tconst': 'imdb_tconst', 'nconst': 'imdb_nconst', 'category': 'category'}
    new_doc = {field_map[key]: val for key, val in doc.items() if key in field_map}

    return new_doc


def transform_title_basics(doc):
    field_map = {'tconst': 'imdb_tconst', 'primaryTitle': 'title'}
    new_doc = {field_map[key]: val for key, val in doc.items() if key in field_map}
    if 'startYear' in doc:
        new_doc['year'] = doc['startYear']
    elif 'endYear' in doc:
        new_doc['year'] = doc['endYear']

    return new_doc


def transform_name_basics(doc):
    field_map = {
        'nconst': 'imdb_nconst',
        'tconst': 'imdb_tconst',
        'birthYear': 'birth_year',
        'deathYear': 'death_year',
        'primaryName': 'name',
    }
    new_doc = {field_map[key]: val for key, val in doc.items() if key in field_map}

    professions = doc.get('primaryProfession', '').split(',')
    if professions:
        new_doc['professions'] = professions

    popular_titles = doc.get('knownForTitles', '').split(',')
    if popular_titles:
        new_doc['popular_titles'] = popular_titles

    return new_doc


start_time = time.time()
execution_date = datetime.strptime('20230118-120000-000000', '%Y%m%d-%H%M%S-%f')
namespace = pomps.namespace(root_dir=DATA_DIR, env=ENV, execution_date=execution_date)

title_principals = pomps.load_and_transform_source_data(
    name='title_principals',
    namespace=namespace,
    transform_func=transform_title_principals,
    load_func=load_imdb_data_func('https://datasets.imdbws.com/title.principals.tsv.gz'),
)

title_basics = pomps.load_and_transform_source_data(
    name='title_basics',
    namespace=namespace,
    transform_func=transform_title_basics,
    load_func=load_imdb_data_func('https://datasets.imdbws.com/title.basics.tsv.gz'),
)

name_basics = pomps.load_and_transform_source_data(
    name='name_basics',
    namespace=namespace,
    transform_func=transform_name_basics,
    load_func=load_imdb_data_func('https://datasets.imdbws.com/name.basics.tsv.gz'),
)

grouped_title_principals = pomps.group_data(
    source_path=title_principals, group_key_func=lambda x: x['imdb_tconst'], group_by_name='imdb_tconst', group_buckets=13
)

grouped_title_basics = pomps.group_data(
    source_path=title_basics, group_key_func=lambda x: x['imdb_tconst'], group_by_name='imdb_tconst', group_buckets=13
)


def title_merge_func(val):
    group_key, basic_data, principal_data = val
    data = []
    for b in basic_data:
        for p in principal_data:
            new_doc = dict(b)
            for key in ['imdb_nconst', 'category']:
                if key not in p:
                    continue
                new_doc[key] = p[key]

            data.append(new_doc)

    return data


title_data = pomps.merge_data_sources(
    name='title_data',
    namespace=namespace,
    data_one_jsonl_path=grouped_title_basics,
    data_two_jsonl_path=grouped_title_principals,
    merge_func=title_merge_func,
)

grouped_name_basics = pomps.group_data(
    source_path=name_basics, group_key_func=lambda x: x['imdb_nconst'], group_by_name='imdb_nconst', group_buckets=13
)
grouped_title_data = pomps.group_data(
    source_path=title_data, group_key_func=lambda x: x['imdb_nconst'], group_by_name='imdb_nconst', group_buckets=13
)


def name_title_merge_func(val):
    group_key, name_data, title_data = val

    if not name_data:
        print(f"[name_title_merge_func] orphan - group_key: {group_key}, title_data: {title_data}")
        return []

    if len(name_data) > 1:
        print(f"[name_title_merge_func] more names than expected.  group_key: {group_key}, name_data: {name_data}")

    new_doc = dict(name_data[0])

    popular_titles = [t for t in title_data if t['imdb_tconst'] in new_doc['popular_titles']]
    if not popular_titles:
        """We don't care about nobodies."""
        return []

    popular_titles = [{key: val for key, val in t.items() if key != 'imdb_nconst'} for t in popular_titles]

    new_doc['popular_titles'] = popular_titles

    return [new_doc]


name_data = pomps.merge_data_sources(
    name='name_data',
    namespace=namespace,
    data_one_jsonl_path=grouped_name_basics,
    data_two_jsonl_path=grouped_title_data,
    merge_func=name_title_merge_func,
)

run_time = int(time.time() - start_time)
print(f"[example] Runtime: {run_time} seconds, {'{0:0.2f}'.format(run_time/60)} minutes")
