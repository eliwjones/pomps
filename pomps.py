import json
import os

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
        """
        We expect the data to be grouped and so we must do this.

        We must create a group folder, and then scan over source_path, generating group_keys and scattering
        documents into buckets via some hash function.

        Then, we must go through each bucket and process/group the documents since we've presumably reduced the RAM
        footprint enough to enable processing said bucket of items.

        NOTE: We must iron fist the case where a group_key is None.. that should not be allowed as it lends itself
        to "hot keys" and RAM explosion.
        """

        grouped_path = f"{namespace}/{name}/grouped/source_data.jsonl"

    with open(transformed_path + '.tmp', 'w', encoding='utf-8') as tmpfile, open(source_path, encoding='utf-8') as source:
        for line in source:
            doc = transform_func(json.loads(line[-1]))
            tmpfile.write(json.dumps(doc) + '\n')

    os.rename(transformed_path + '.tmp', transformed_path)

    return transformed_path


def generate_sorted_join_key_index(data_path):
    pass


def group_data(data_path, group_key_func):
    pass


def fixed_hash(value):
    import hashlib

    if not isinstance(value, str):
        raise Exception(f"[fixed_hash] we expect string values.  Neither ints nor None allowed.  value: {value}")

    return int(hashlib.sha512(value.encode('utf-8')).hexdigest(), 16)


def serialize_execution_date(execution_date):
    return execution_date.strftime('%Y%m%d-%H%M%S-%f')
