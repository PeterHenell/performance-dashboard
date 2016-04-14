from collection_manager import CollectionManager
from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
from query_store import QueryStore
import unittest

from sqlserver_collector import SQLServerCollector

manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


def find(f, seq):
    for item in seq:
        if f(item):
            return item


class MockDb:

    def __init__(self, db_config):
        self.config = db_config

    def get_records(self, query):
        # print(self.records)
        # print(self.records[query])
        return MockDb.records[query].pop(0)

    @staticmethod
    def set_records(records):
        MockDb.records = records


class CollectionManagerTestCase(unittest.TestCase):
    def test_should_calculate_values(self):
        prev, delta = DataCollector.calculate_values(15, 2)
        self.assertEquals(2, prev)
        self.assertEquals(13, delta)

    def test_should_calculate_row_delta(self):
        row = {'ID': 111, 'bytes': 100}
        cached_row = {'ID': 111, 'bytes': 40}
        data_key_col = 'ID'
        row_delta = DataCollector.calculate_row_delta(row, cached_row, data_key_col)

        self.assertEquals(row_delta, {'ID': 111, 'bytes_delta': 60, 'bytes_measured': 100, 'bytes_prev': 40})

    def test_from_query(self):
        mock_db = MockDb(None)
        MockDb.records = {'mocked query': [
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 1, 'total_bytes': 58}]
        ]}
        collector = SQLServerCollector.from_query(mock_db,
                                                  'this is a query name',
                                                  'mocked query',
                                                  'mocked_key_col',
                                                  'collector_name')
        self.assertEquals(collector.query_name, 'this is a query name')
        delta1 = collector.get_delta()
        self.assertListEqual(delta1, [{'mocked_key_col': 1, 'total_bytes_measured': 23}])

        delta2 = collector.get_delta()
        self.assertListEqual(delta2, [{'mocked_key_col': 1,
                                       'total_bytes_measured': 58,
                                       'total_bytes_prev': 23,
                                       'total_bytes_delta': 35}])

    def test_should_create_collectors_for_all_queries(self):
        MockDb.records = {
            'sql 1': [[{'mocked_key_col': 1, 'total_bytes': 23}]],
            'sql 2': [[{'mocked_key_col': 1, 'total_bytes': 58}]]
        }
        queries = {'query name1': {'sql_text': 'sql 1', 'key_col': 'cola'},
                   'query_name 2': {'sql_text': 'sql 2', 'key_col': 'col k'}}
        collection_manager = CollectionManager()
        sql_coll = SQLServerCollector(MockDb, queries)
        collection_manager.collectors.extend(sql_coll.get_collectors('', config))

        q1 = find(lambda collector: collector.query_name == 'query name1', collection_manager.collectors)
        q2 = find(lambda collector: collector.query_name == 'query_name 2', collection_manager.collectors)

        self.assertEquals(q1.data_key_col, 'cola')
        self.assertEquals(q2.data_key_col, 'col k')

    def test_should_collect_data_from_all_queries(self):
        MockDb.records = {'mocked query': [
            [{'mocked_key_col': 1, 'total_bytes': 44}],
            [{'mocked_key_col': 1, 'total_bytes': 58}],
        ],
            'mocked query 2': [
                [{'mocked_key_col': 2, 'total_bytes': 23}],
                [{'mocked_key_col': 2, 'total_bytes': 58}]
            ]}
        queries = {'query name1': {'sql_text': 'mocked query', 'key_col': 'mocked_key_col'},
                   'query_name 2': {'sql_text': 'mocked query 2', 'key_col': 'mocked_key_col'}}
        collection_manager = CollectionManager()
        sqlcol = SQLServerCollector(MockDb, queries)
        collection_manager.collectors.extend(sqlcol.get_collectors('mockollector', config))

        delta = collection_manager.collect_data()

        big_delta = collection_manager.collect_data()

        query1_delta = find(lambda d: d['query_name'] == 'query name1', big_delta)
        query2_delta = find(lambda d: d['query_name'] == 'query_name 2', big_delta)

        expected1 = {'query_name': 'query name1', 'delta': [{'mocked_key_col': 1,
                                                             'total_bytes_delta': 14,
                                                             'total_bytes_measured': 58,
                                                             'total_bytes_prev': 44}]}
        expected2 = {'query_name': 'query_name 2', 'delta': [{'mocked_key_col': 2,
                                                              'total_bytes_delta': 35,
                                                              'total_bytes_measured': 58,
                                                              'total_bytes_prev': 23}]}
        self.assertEqual(expected1, query1_delta)
        self.assertEqual(expected2, query2_delta)

    def test_should_initialize_from_QueryStore(self):
        MockDb.records = [
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 2, 'total_bytes': 58}],
            [{'mocked_key_col': 1, 'total_bytes': 23}],
            [{'mocked_key_col': 2, 'total_bytes': 58}]
        ]

        collection_manager = CollectionManager()
        sqlcol = SQLServerCollector(MockDb)
        collection_manager.collectors = sqlcol.get_collectors('mockollector', config)
        self.assertTrue(len(collection_manager.collectors) > 2)


if __name__ == '__main__':
    unittest.main()
