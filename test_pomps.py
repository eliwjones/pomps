import pomps

import json
import shutil
import unittest

from pathlib import Path

TEST_DATA = './data/test'


class TestPomps(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(TEST_DATA, ignore_errors=True)

    def test_group_data(self):
        test_jsonl = [
            {'_id': 0, 'name': 'bob smith', 'type': 'player'},
            {'_id': 0, 'name': 'robert smith', 'type': 'pimp'},
            {'_id': 2, 'name': 'joe j', 'type': 'player'},
            {'_id': 1, 'name': 'bill b', 'type': 'witness'},
            {'_id': 0, 'name': 'rsmith', 'type': 'witness'},
        ]

        jsonl_path = f"{TEST_DATA}/test.jsonl"
        Path(jsonl_path).parent.mkdir(parents=True, exist_ok=True)
        Path(jsonl_path).write_text('\n'.join(map(json.dumps, test_jsonl)))

        def group_key_func(data):
            return str(data['_id'])

        grouped_path = f"{TEST_DATA}/grouped_test.jsonl"
        pomps.group_data(source_path=jsonl_path, grouped_path=grouped_path, group_key_func=group_key_func, group_buckets=2)

        expected = '\n'.join(
            [
                '{"group_key": "0", "data": [{"_id": 0, "name": "bob smith", "type": "player"}, {"_id": 0, "name": "robert smith", "type": "pimp"}, {"_id": 0, "name": "rsmith", "type": "witness"}]}',
                '{"group_key": "2", "data": [{"_id": 2, "name": "joe j", "type": "player"}]}',
                '{"group_key": "1", "data": [{"_id": 1, "name": "bill b", "type": "witness"}]}',
                '',
            ]
        )

        self.assertEqual(Path(grouped_path).read_text(), expected)


if __name__ == '__main__':
    unittest.main(verbosity=2)