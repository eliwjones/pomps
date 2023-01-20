import glob
import json
import os
import shutil

from pathlib import Path

DEBUG_MODULUS = 369_369


def load_and_transform_source_data(name, namespace, transform_func, load_func, group_key_func=None, group_buckets=10):
    source_path = f"{namespace}/{name}/source_data.jsonl"
    transformed_path = f"{namespace}/{name}/transformed_source_data.jsonl"

    Path(source_path).parent.mkdir(parents=True, exist_ok=True)

    if Path(transformed_path).is_file():
        print(f"[load_source_data] data already loaded and transformed.  Returning: {transformed_path}")

        return transformed_path

    if not Path(source_path).is_file():
        print(f"[load_source_data] source data '{source_path}' not yet loaded, retrieving it using provided load_func().")

        load_func(filepath=source_path + '.tmp')
        Path(source_path + '.tmp').rename(source_path)

    if group_key_func:
        grouped_path = group_data(source_path, group_key_func, group_buckets)

        source_path = grouped_path

    with open(transformed_path + '.tmp', 'w', encoding='utf-8') as tmpfile, open(source_path, encoding='utf-8') as source:
        counter = 0
        for line in source:
            counter += 1
            doc = transform_func(json.loads(line.rstrip()))
            tmpfile.write(json.dumps(doc) + '\n')

            if not counter % DEBUG_MODULUS:
                print(f"[load_and_transform_source_data] transformed {counter} docs.")

    Path(transformed_path + '.tmp').rename(transformed_path)

    return transformed_path


def group_data(source_path, group_key_func, group_buckets, group_by_name=''):
    """
    TODO: group_buckets number should be calculated in here based on Available RAM and source_path file_size.
    """

    source_filename = source_path.split('/')[-1]

    """
      Feels a little funky using this group_by_name.  It is a workaround for calling group_data on multiple
      source_path files in the same parent folder.

      TODO: Ponder the cleaner way.
    """
    if group_by_name:
        group_by_name += '/'
        if group_by_name[0] != '_':
            group_by_name = '_' + group_by_name

    grouped_path = source_path.replace(source_filename, f"{group_by_name}grouped_source_data.jsonl")
    Path(grouped_path).parent.mkdir(parents=True, exist_ok=True)

    if Path(grouped_path).is_file():
        print(f"[group_data] found existing data, returning: {grouped_path}")
        return grouped_path

    """
    If group_buckets > 1, we will generate all of our buckets and assign them to the buckets var.
    """
    grouped_file = grouped_path.split('/')[-1]
    buckets_path = grouped_path.replace(grouped_file, 'buckets')
    if group_buckets > 1 and not Path(buckets_path).is_dir():
        tmp_buckets_path = f"{buckets_path}_tmp"

        shutil.rmtree(tmp_buckets_path, ignore_errors=True)
        Path(tmp_buckets_path).mkdir(parents=True, exist_ok=True)

        sorted_keys = get_and_sort_keys(jsonl_path=source_path, key_func=group_key_func)
        bucket_map = generate_bucket_map(keys=sorted_keys, buckets=group_buckets)

        print(f"[group_data] bucket_map: {bucket_map}")

        bucket_file_handles = {}

        try:
            """
            Generally, we would prefer to just use 'with open()' down where we are doing our
            'write(line)', but opening a file to append just one line is very slow in Windows.

            Thus, we have optimized to use this dict of open file handles that get closed in the
            finally block.
            """

            for bucket in bucket_map:
                bucket_path = f"{tmp_buckets_path}/{bucket}.jsonl"
                bucket_file_handles[bucket] = open(bucket_path, 'a', encoding='utf-8')

            with open(source_path, encoding='utf-8') as source:
                counter = 0
                for line in source:
                    counter += 1
                    group_key = group_key_func(json.loads(line.rstrip()))

                    bucket = get_bucket(key=group_key, bucket_map=bucket_map)
                    bucket_file_handles[bucket].write(line)

                    if not counter % DEBUG_MODULUS:
                        print(f"[group_data] bucketed {counter} docs from source_path: {source_path}.")
        finally:
            for bucket in bucket_file_handles:
                bucket_file_handles[bucket].close()

        Path(tmp_buckets_path).replace(buckets_path)

    Path(grouped_path + '.tmp').unlink(missing_ok=True)

    buckets = [source_path]
    if group_buckets > 1:
        buckets = glob.glob(f"{buckets_path}/*.jsonl")
        buckets = sorted(buckets, key=lambda x: x.split('/')[-1].replace('.jsonl', '').split('_'))

    write_counter = 0
    for bucket_path in buckets:
        grouped_data = {}

        with open(bucket_path, encoding='utf-8') as b:
            group_counter = 0
            for line in b:
                group_counter += 1
                if line == '\n':
                    continue

                data = json.loads(line.rstrip())
                group_key = group_key_func(data)

                if group_key not in grouped_data:
                    grouped_data[group_key] = []

                grouped_data[group_key].append(data)

                if not group_counter % DEBUG_MODULUS:
                    print(f"[group_data] grouped {group_counter} docs for bucket: {bucket_path}.")

        sorted_keys = sorted(grouped_data.keys())
        with open(grouped_path + '.tmp', 'a', encoding='utf-8') as tmpfile:
            for group_key in sorted_keys:
                write_counter += 1
                line = {'group_key': group_key, 'data': grouped_data[group_key]}
                tmpfile.write(json.dumps(line) + '\n')

                if not write_counter % DEBUG_MODULUS:
                    print(f"[group_data] written {write_counter} groups to {grouped_path}.tmp")

    Path(grouped_path + '.tmp').rename(grouped_path)

    return grouped_path


def fixed_hash(value):
    import hashlib

    if not isinstance(value, str):
        raise Exception(f"[fixed_hash] we expect string values.  Neither ints nor None allowed.  value: {value}")

    return int(hashlib.sha512(value.encode('utf-8')).hexdigest(), 16)


def generate_bucket_map(keys, buckets):
    size = len(keys) // buckets
    bucket_map = {}

    start, end, counter, skip_key = None, None, 0, None
    for key in keys:
        if key == skip_key:
            continue

        counter += 1
        end = key
        if start is None:
            start = key

        if counter != size:
            continue

        if start == end:
            """
            We have a hot key that filled up an entire bucket.  To reflect the reality that this bucket will get all of these
            keys, we shall skip until the next key is found.
            """
            skip_key = end

        bucket_map[f"{start}_{end}"] = (start, end)
        start, end, counter = None, None, 0

    if counter != size and start and end:
        bucket_map[f"{start}_{end}"] = (start, end)

    return bucket_map


def get_bucket(key, bucket_map):
    for bucket in bucket_map:
        if bucket_map[bucket][0] <= key <= bucket_map[bucket][1]:
            return bucket

    raise Exception(f"[get_bucket] key: {key}  We created a bucket_map that does not know what to do with our key!")


def get_and_sort_keys(jsonl_path, key_func):
    keys = []
    with open(jsonl_path, encoding='utf-8') as source:
        counter = 0
        for line in source:
            counter += 1
            line = line.rstrip()
            if not line:
                continue

            keys.append(str(key_func(json.loads(line))))

            if not counter % DEBUG_MODULUS:
                print(f"[get_and_sort_keys] gathered {counter} keys.")

    return sorted(keys)


def load_line(f):
    batch = {'group_key': None, 'data': []}
    line = f.readline()
    if line:
        batch = json.loads(line[:-1])
    return batch


def merge_data_sources(name, namespace, data_one_jsonl_path, data_two_jsonl_path, merge_func):
    merged_jsonl_path = f"{namespace}/{name}/merged.jsonl"
    Path(merged_jsonl_path).parent.mkdir(parents=True, exist_ok=True)

    if Path(merged_jsonl_path).is_file():
        return merged_jsonl_path

    counter = 0
    data_one_orphans = 0
    data_two_orphans = 0
    merge_count = 0

    workfile = f"{merged_jsonl_path}.tmp"
    with open(data_one_jsonl_path, encoding='utf-8') as data_one, open(data_two_jsonl_path, encoding='utf-8') as data_two, open(
        workfile, 'w', encoding='utf-8'
    ) as output:

        data_one_batch = load_line(data_one)
        data_two_batch = load_line(data_two)

        while data_one_batch['group_key'] is not None or data_two_batch['group_key'] is not None:
            if data_one_batch['group_key'] == data_two_batch['group_key']:
                val = (data_one_batch['group_key'], data_one_batch['data'], data_two_batch['data'])
                emit_json = [json.dumps(line) + '\n' for line in merge_func(val)]

                emit_count = len(emit_json)

                counter += emit_count
                merge_count += emit_count

                if emit_json:
                    output.writelines(emit_json)

                data_one_batch = load_line(data_one)
                data_two_batch = load_line(data_two)
            elif data_two_batch['group_key'] is None or (
                data_one_batch['group_key'] and (data_one_batch['group_key'] < data_two_batch['group_key'])
            ):
                """
                This is a pitiable logic check.. which would be simpler with a max value.. but.. we can have no
                knowable max str value.
                """

                val = (data_one_batch['group_key'], data_one_batch['data'], [])
                emit_json = [json.dumps(line) + '\n' for line in merge_func(val)]

                emit_count = len(emit_json)

                counter += emit_count

                if emit_json:
                    output.writelines(emit_json)

                data_one_batch = load_line(data_one)
            else:
                val = (data_two_batch['group_key'], [], data_two_batch['data'])
                emit_json = [json.dumps(line) + '\n' for line in merge_func(val)]

                emit_count = len(emit_json)

                counter += emit_count

                if emit_json:
                    output.writelines(emit_json)

                data_two_batch = load_line(data_two)

            if not counter % DEBUG_MODULUS:
                print(f"[merge_data_sources] counter: {counter}, merge_count: {merge_count} for {merged_jsonl_path}")

    Path(workfile).rename(merged_jsonl_path)

    return merged_jsonl_path


def namespace(root_dir, env, execution_date):
    namespace = f"{root_dir}/{env}/{serialize_execution_date(execution_date)}"

    return namespace


def serialize_execution_date(execution_date):
    return execution_date.strftime('%Y%m%d-%H%M%S-%f')
