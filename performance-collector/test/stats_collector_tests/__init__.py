import unittest

from config_manager import ConfigManager
from data_collector import DataCollector
from stats_collector import StatCollector


class MockDb:
    def __init__(self, config_manager=None):
        self.records = {'mocked query': [
            [{'mocked_key_col': 1, 'total_bytes': 44}],
            [{'mocked_key_col': 1, 'total_bytes': 58}],
        ],
            'mocked query 2': [
                [{'mocked_key_col': 2, 'total_bytes': 23}],
                [{'mocked_key_col': 2, 'total_bytes': 58}]
            ]}
        self.db_name = 'mock database name'

    @staticmethod
    def collector_from_query(db, query_name, query_text, key_column, source_type):
        return DataCollector(lambda: db.get_records(query_text), key_column, query_name, source_type)

    @staticmethod
    def collectors_from_queries(db, queries, collector_name):
        collectors = []
        for name, query in queries.items():
            collectors.append(MockDb.collector_from_query(db, name, query['sql_text'], query['key_col'], 'source type'))
        return collectors

    def get_records(self, query):
        return self.records[query].pop(0)


class MockElasticSearchAPI:
    def __init__(self, config_manager=None):
        self.records = {}
        self.db_name = ''
        self.query_name = ''

    def consume_to_index(self, records, db_name, query_name, timestamp):
        index = 'hist-%s-%s' % (db_name, query_name)
        doc_type = query_name + '_type'
        print('ElasticMock consume_to_index: %s.%s [%s]' % (index, doc_type, records))
        self.records[index + doc_type] = records

    def initialize_elasticsearch(self, db_name, query_name):
        print('ElasticMock initIndex %s.%s' % (db_name, query_name))
        self.db_name = db_name
        self.query_name = query_name


class StatsCollectorTests(unittest.TestCase):

    def test_should_use_mock_api(self):
        queries = {'query name1': {'sql_text': 'mocked query', 'key_col': 'mocked_key_col'},
                   'query_name 2': {'sql_text': 'mocked query 2', 'key_col': 'mocked_key_col'}}
        db = MockDb()
        es = MockElasticSearchAPI()
        collectors = MockDb.collectors_from_queries(db, queries, 'mock source type')

        sc = StatCollector(es, collectors)

        sc.run()
        self.assertEquals(2, len(sc.api.records), 'All runs return measured values')

        sc.run()
        self.assertEquals(2, len(sc.api.records), 'Second run should have the delta of the two queries')

    def test_should_create_statCollectors_from_config(self):
        config_manager = ConfigManager.from_file('test.ini')
        # queries = {'query name1': {'sql_text': 'mocked query', 'key_col': 'mocked_key_col'},
        #            'query_name 2': {'sql_text': 'mocked query 2', 'key_col': 'mocked_key_col'}}

        stat_collector = StatCollector.from_config_manager(config_manager, MockElasticSearchAPI)
        self.assertTrue(stat_collector is not None)

    def test_should_crate_statsCollector_from_config_with_different_collectorClasses(self):
        config_manager = ConfigManager.from_file('test.ini')
        sc = StatCollector.from_config_manager(config_manager)
        self.assertIsNotNone(sc)
        self.assertTrue(len(sc.collection_manager.collectors) > 0)
        self.fail('Not implemented, issue 1')

if __name__ == '__main__':
    unittest.main()
