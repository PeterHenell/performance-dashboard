from collection_manager import CollectionManager
from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
from query_store import QueryStore
import unittest

manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


def find(f, seq):
    for item in seq:
        if f(item):
            return item


class MockDb:
    def __init__(self, records):
        self.records = records

    def get_records(self, query):
        if query != 'mocked query':
            raise Exception('query parameter was not passed to the get_records function')
        return self.records.pop(0)


class CollectionManagerTestCase(unittest.TestCase):
    def test_from_query(self):
        mock_db = MockDb([
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 1, 'total_bytes': 58}]
        ])
        collector = DataCollector.from_query(mock_db,
                                             'this is a query name',
                                             'mocked query',
                                             'mocked_key_col')
        self.assertEquals(collector.query_name, 'this is a query name')
        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertListEqual(delta2, [{'mocked_key_col': 1, 'total_bytes': 35}])

    def test_should_create_collectors_for_all_queries(self):
        mock_db = MockDb([
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 1, 'total_bytes': 58}]
        ])
        queries = {'query name1': {'sql_text': 'sql 1', 'key_col': 'cola'},
                   'query_name 2': {'sql_text': 'sql 2', 'key_col': 'col k'}}
        collection_manager = CollectionManager(mock_db, queries)

        q1 = find(lambda collector: collector.query_name == 'query name1', collection_manager.collectors)
        q2 = find(lambda collector: collector.query_name == 'query_name 2', collection_manager.collectors)

        self.assertEquals(q1.data_key_col, 'cola')
        self.assertEquals(q2.data_key_col, 'col k')

    def test_should_collect_data_from_all_queries(self):
        mock_db = MockDb([
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 2, 'total_bytes': 58}],
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 2, 'total_bytes': 58}]
        ])
        queries = {'query name1': {'sql_text': 'mocked query', 'key_col': 'mocked_key_col'},
                   'query_name 2': {'sql_text': 'mocked query', 'key_col': 'mocked_key_col'}}
        collection_manager = CollectionManager(mock_db, queries)

        delta = collection_manager.collect_data()
        self.assertEquals(delta, [])

        # [{'delta': [{'total_bytes': 0, 'mocked_key_col': 1}], 'query_name': 'query name1'},
        #  {'delta': [{'total_bytes': 0, 'mocked_key_col': 2}], 'query_name': 'query_name 2'}]
        big_delta = collection_manager.collect_data()

        query1_delta = find(lambda d: d['query_name'] == 'query name1', big_delta)
        query2_delta = find(lambda d: d['query_name'] == 'query_name 2', big_delta)

        self.assertEqual({'query_name': 'query name1', 'delta': [{'total_bytes': 0, 'mocked_key_col': 1}]}, query1_delta)
        self.assertEqual({'query_name': 'query_name 2', 'delta': [{'total_bytes': 0, 'mocked_key_col': 2}]}, query2_delta)

    def test_should_initialize_from_QueryStore(self):
        mock_db = MockDb([
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 2, 'total_bytes': 58}],
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 2, 'total_bytes': 58}]
        ])

        collection_manager = CollectionManager(mock_db, QueryStore.queries)
        self.assertEqual(len(collection_manager.collectors), 3)

if __name__ == '__main__':
    unittest.main()
