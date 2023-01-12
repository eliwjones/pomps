import glob
import json
import os
import shutil

from pathlib import Path


def load_and_transform_source_data(
    name, transform_func, load_func, env, execution_date, root_dir, group_key_func=None, group_buckets=10
):
    """
    TODO: One might wish to group source_data, depending on its structure.  We would, of course, like to avoid
      this if at all possible since it requires slurping all data into RAM, unless one implements a scatter
      algorithm to bucket lines by group_key before grouping in batches.

    TODO: Feels like we will need to do this because grouping is just a fact of life later in the process.
    """
    namespace = f"{root_dir}/{env}/{serialize_execution_date(execution_date)}"

    source_path = f"{namespace}/{name}/source_data.jsonl"
    transformed_path = f"{namespace}/{name}/transformed_source_data.jsonl"

    Path(source_path).parent.mkdir(parents=True, exist_ok=True)

    if Path(transformed_path).is_file():
        print(f"[load_source_data] data already loaded and transformed.  Returning: {transformed_path}")

        return transformed_path

    if not Path(source_path).is_file():
        print(f"[load_source_data] source data '{source_path}' not yet loaded, retrieving it using provided load_func().")

        load_func(filepath=source_path + '.tmp')
        os.rename(source_path + '.tmp', source_path)

    if group_key_func:
        grouped_path = f"{namespace}/{name}/grouped_source_data.jsonl"
        if not Path(grouped_path).is_file():
            group_data(source_path, grouped_path, group_key_func, group_buckets)

        source_path = grouped_path

    with open(transformed_path + '.tmp', 'w', encoding='utf-8') as tmpfile, open(source_path, encoding='utf-8') as source:
        for line in source:
            doc = transform_func(json.loads(line.rstrip()))
            tmpfile.write(json.dumps(doc) + '\n')

    os.rename(transformed_path + '.tmp', transformed_path)

    return transformed_path


def generate_sorted_join_key_index(data_path):
    pass


def group_data(source_path, grouped_path, group_key_func, group_buckets):
    grouped_file = grouped_path.split('/')[-1]
    buckets_path = grouped_path.replace(grouped_file, 'buckets')

    if not Path(buckets_path).is_dir():
        tmp_buckets_path = f"{buckets_path}_tmp"

        shutil.rmtree(tmp_buckets_path, ignore_errors=True)
        Path(tmp_buckets_path).mkdir(parents=True, exist_ok=True)

        with open(source_path, encoding='utf-8') as source:
            for line in source:
                group_key = group_key_func(json.loads(line.rstrip()))
                bucket = str(fixed_hash(group_key) % group_buckets).zfill(len(str(group_buckets)))
                bucket_path = f"{tmp_buckets_path}/{bucket}.jsonl"

                with open(bucket_path, 'a', encoding='utf-8') as f:
                    f.write(line)

        shutil.move(tmp_buckets_path, buckets_path)

    Path(grouped_path + '.tmp').unlink(missing_ok=True)

    for bucket_path in glob.glob(f"{buckets_path}/*"):
        grouped_data = {}

        with open(bucket_path, encoding='utf-8') as b:
            for line in b:
                if line == '\n':
                    continue

                data = json.loads(line.rstrip())
                group_key = group_key_func(data)

                if group_key not in grouped_data:
                    grouped_data[group_key] = []

                grouped_data[group_key].append(data)

        with open(grouped_path + '.tmp', 'a', encoding='utf-8') as tmpfile:
            for group_key in grouped_data:
                line = {'group_key': group_key, 'data': grouped_data[group_key]}
                tmpfile.write(json.dumps(line) + '\n')

    os.rename(grouped_path + '.tmp', grouped_path)

    return grouped_path


def fixed_hash(value):
    import hashlib

    if not isinstance(value, str):
        raise Exception(f"[fixed_hash] we expect string values.  Neither ints nor None allowed.  value: {value}")

    return int(hashlib.sha512(value.encode('utf-8')).hexdigest(), 16)


def serialize_execution_date(execution_date):
    return execution_date.strftime('%Y%m%d-%H%M%S-%f')
