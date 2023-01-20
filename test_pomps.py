import pomps

import json
import shutil
import unittest

from datetime import datetime
from pathlib import Path

TEST_DATA = './data/test'


class TestPomps(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TEST_DATA, ignore_errors=True)

    def test_group_data(self):
        test_jsonl = [
            {'_id': 2, 'name': 'joe j', 'type': 'player'},
            {'_id': 0, 'name': 'bob smith', 'type': 'player'},
            {'_id': 0, 'name': 'robert smith', 'type': 'pimp'},
            {'_id': 1, 'name': 'bill b', 'type': 'witness'},
            {'_id': 0, 'name': 'rsmith', 'type': 'witness'},
        ]

        jsonl_path = f"{TEST_DATA}/test.jsonl"
        Path(jsonl_path).parent.mkdir(parents=True, exist_ok=True)
        Path(jsonl_path).write_text('\n'.join(map(json.dumps, test_jsonl)))

        def group_key_func(data):
            return str(data['_id'])

        for group_buckets in [1, 2]:
            grouped_path = pomps.group_data(
                source_path=jsonl_path, group_key_func=group_key_func, group_buckets=group_buckets, group_by_name='_id'
            )
            expected = '\n'.join(
                [
                    json.dumps(
                        {
                            'group_key': '0',
                            'data': [
                                {'_id': 0, 'name': 'bob smith', 'type': 'player'},
                                {'_id': 0, 'name': 'robert smith', 'type': 'pimp'},
                                {'_id': 0, 'name': 'rsmith', 'type': 'witness'},
                            ],
                        }
                    ),
                    json.dumps({'group_key': '1', 'data': [{'_id': 1, 'name': 'bill b', 'type': 'witness'}]}),
                    json.dumps({'group_key': '2', 'data': [{'_id': 2, 'name': 'joe j', 'type': 'player'}]}),
                    '',
                ]
            )

            self.assertEqual(Path(grouped_path).read_text(), expected, f"Failed for group_buckets={group_buckets}")

    def test_load_and_transform_source_data(self):
        name = 'some_name_for_data'

        def transform_func(data):
            new_data = {'id': data['_id'], 'name': data['name'].title(), 'classification': data['type'].replace('pimp', 'p*mp')}
            return new_data

        def load_func(filepath):
            test_jsonl = [
                {'_id': 0, 'name': 'robert smith', 'type': 'pimp'},
                {'_id': 2, 'name': 'joe j', 'type': 'player'},
                {'_id': 1, 'name': 'bill b', 'type': 'witness'},
            ]

            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            Path(filepath).write_text('\n'.join(map(json.dumps, test_jsonl)))

        namespace = pomps.namespace(root_dir=TEST_DATA, env='testing', execution_date=datetime.now())

        transformed_path = pomps.load_and_transform_source_data(
            name=name, namespace=namespace, transform_func=transform_func, load_func=load_func
        )

        expected = '\n'.join(
            [
                json.dumps({'id': 0, 'name': 'Robert Smith', 'classification': 'p*mp'}),
                json.dumps({'id': 2, 'name': 'Joe J', 'classification': 'player'}),
                json.dumps({'id': 1, 'name': 'Bill B', 'classification': 'witness'}),
                '',
            ]
        )

        self.assertEqual(Path(transformed_path).read_text(), expected)

    def test_load_and_transform_source_data_with_grouping(self):
        name = 'some_name_for_data'

        def transform_func(data):
            new_data = {'id': None, 'names': [], 'classifications': []}
            for d in data['data']:
                new_data['id'] = d['_id']
                new_data['names'].append(d['name'].title())
                new_data['classifications'].append(d['type'].replace('pimp', 'p*mp'))

            return new_data

        def load_func(filepath):
            test_jsonl = [
                {'_id': 0, 'name': 'bob smith', 'type': 'player'},
                {'_id': 0, 'name': 'robert smith', 'type': 'pimp'},
                {'_id': 2, 'name': 'joe j', 'type': 'player'},
                {'_id': 1, 'name': 'bill b', 'type': 'witness'},
                {'_id': 0, 'name': 'rsmith', 'type': 'witness'},
            ]

            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            Path(filepath).write_text('\n'.join(map(json.dumps, test_jsonl)))

        def group_key_func(data):
            return str(data['_id'])

        namespace = pomps.namespace(root_dir=TEST_DATA, env='testing', execution_date=datetime.now())
        transformed_path = pomps.load_and_transform_source_data(
            name=name,
            namespace=namespace,
            transform_func=transform_func,
            load_func=load_func,
            group_key_func=group_key_func,
            group_buckets=2,
        )

        expected = '\n'.join(
            [
                json.dumps({'id': 0, 'names': ['Bob Smith', 'Robert Smith', 'Rsmith'], 'classifications': ['player', 'p*mp', 'witness']}),
                json.dumps({'id': 1, 'names': ['Bill B'], 'classifications': ['witness']}),
                json.dumps({'id': 2, 'names': ['Joe J'], 'classifications': ['player']}),
                '',
            ]
        )

        self.assertEqual(Path(transformed_path).read_text(), expected)

    def test_merge_data_sources(self):
        self.maxDiff = None
        """Load two data sets."""

        def load_func_one(filepath):
            test_jsonl = [
                {'_id': 0, 'name': 'bob smith', 'type': 'player'},
                {'_id': 0, 'name': 'robert smith', 'type': 'pimp'},
                {'_id': 2, 'name': 'joe j', 'type': 'player'},
                {'_id': 1, 'name': 'bill b', 'type': 'witness'},
                {'_id': 0, 'name': 'rsmith', 'type': 'witness'},
            ]

            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            Path(filepath).write_text('\n'.join(map(json.dumps, test_jsonl)))

        def transform_func_one(data):
            new_data = {'id': None, 'name': data['data'][0]['name'].title(), 'names': [], 'classifications': []}
            for d in data['data']:
                new_data['id'] = d['_id']
                new_data['names'].append(d['name'].title())
                new_data['classifications'].append(d['type'].replace('pimp', 'p*mp'))

            return new_data

        def group_key_func_one(data):
            return str(data['_id'])

        def load_func_two(filepath):
            test_jsonl = [
                {'control_num': 'A21', 'name': 'bob smith', 'address': '123 Street St, California, CA'},
                {'control_num': 'B33', 'name': 'joe j', 'address': '321 Road Rd, Texas, TX'},
                {'control_num': 'C7', 'name': 'bill b', 'address': '111 Avenue Ave, NY, NY'},
            ]

            Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            Path(filepath).write_text('\n'.join(map(json.dumps, test_jsonl)))

        def transform_func_two(data):
            new_data = {'control_number': data['control_num'], 'name': data['name'].title(), 'address': data['address']}
            return new_data

        execution_date = datetime.now()
        namespace = pomps.namespace(root_dir=TEST_DATA, env='testing', execution_date=execution_date)

        transformed_path_one = pomps.load_and_transform_source_data(
            name='data_one',
            namespace=namespace,
            transform_func=transform_func_one,
            load_func=load_func_one,
            group_key_func=group_key_func_one,
            group_buckets=2,
        )

        transformed_path_two = pomps.load_and_transform_source_data(
            name='data_two', namespace=namespace, transform_func=transform_func_two, load_func=load_func_two
        )

        """ Create grouped, sorted indexes """

        sorted_index_one = pomps.group_data(
            source_path=transformed_path_one, group_key_func=lambda x: x['name'], group_buckets=2, group_by_name='name'
        )
        sorted_index_two = pomps.group_data(
            source_path=transformed_path_two, group_key_func=lambda x: x['name'], group_buckets=2, group_by_name='name'
        )

        """ merge """

        def merge_func(val):
            group_key, data_one, data_two = val
            data = []
            for doc_one in data_one:
                for doc_two in data_two:
                    if 'address' not in doc_one:
                        doc_one['address'] = []
                    doc_one['address'].append(doc_two['address'])
                    doc_one['control_number'] = doc_two['control_number']

                data.append(doc_one)

            return data

        merged_jsonl_path = pomps.merge_data_sources(
            name='one_and_two',
            namespace=namespace,
            data_one_jsonl_path=sorted_index_one,
            data_two_jsonl_path=sorted_index_two,
            merge_func=merge_func,
        )

        expected = '\n'.join(
            [
                json.dumps(
                    {
                        'id': 1,
                        'name': 'Bill B',
                        'names': ['Bill B'],
                        'classifications': ['witness'],
                        'address': ['111 Avenue Ave, NY, NY'],
                        'control_number': 'C7',
                    }
                ),
                json.dumps(
                    {
                        'id': 0,
                        'name': 'Bob Smith',
                        'names': ['Bob Smith', 'Robert Smith', 'Rsmith'],
                        'classifications': ['player', 'p*mp', 'witness'],
                        'address': ['123 Street St, California, CA'],
                        'control_number': 'A21',
                    }
                ),
                json.dumps(
                    {
                        'id': 2,
                        'name': 'Joe J',
                        'names': ['Joe J'],
                        'classifications': ['player'],
                        'address': ['321 Road Rd, Texas, TX'],
                        'control_number': 'B33',
                    }
                ),
                '',
            ]
        )

        self.assertEqual(Path(merged_jsonl_path).read_text(), expected)


if __name__ == '__main__':
    unittest.main(verbosity=2)
