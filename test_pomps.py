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
            grouped_path = pomps.group_data(source_path=jsonl_path, group_key_func=group_key_func, group_buckets=group_buckets)
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

        transformed_path = pomps.load_and_transform_source_data(
            name, transform_func, load_func, env='testing', execution_date=datetime.now(), root_dir=TEST_DATA
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

        transformed_path = pomps.load_and_transform_source_data(
            name,
            transform_func,
            load_func,
            env='testing',
            execution_date=datetime.now(),
            root_dir=TEST_DATA,
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
