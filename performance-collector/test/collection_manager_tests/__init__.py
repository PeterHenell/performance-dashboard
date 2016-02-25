from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
from query_store import QueryStore
import unittest

manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


class MockDb:
    def __init__(self, records):
        self.records = records

    def get_records(self, query):
        return self.records.pop(0)


class CollectionManagerTestCase(unittest.TestCase):
    def test_from_query(self):
        mock_db = MockDb([
            [{'mocked_key_col': 1, 'total_bytes': 0}],
            [{'mocked_key_col': 1, 'total_bytes': 78}]
        ])
        collector = DataCollector.from_query(mock_db,
                                             'mocked query',
                                             'mocked_key_col')
        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertListEqual(delta2, [{'mocked_key_col': 1, 'total_bytes': 78}])


if __name__ == '__main__':
    unittest.main()
