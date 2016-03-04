from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
from query_store import QueryStore
import unittest


manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


# def db_collector():
#     return db.get_records(QueryStore.get_query_text('dm_io_virtual_file_stats'))


def mock_collect():
    collect = [{'Col': 1, 'total_ms': mock_collect.counter, 'total_bytes': mock_collect.counter * 100}]
    print('Collect: %s' % collect)
    mock_collect.counter += 2
    return collect

mock_collect.counter = 0


def multirow_mock_collect():
    collect = [
        {'Col': 1, 'total_ms': multirow_mock_collect.counter, 'total_bytes': multirow_mock_collect.counter * 100},
        {'Col': 2, 'total_ms': multirow_mock_collect.counter * 2, 'total_bytes': multirow_mock_collect.counter * 200},
        {'Col': 3, 'total_ms': multirow_mock_collect.counter * 3, 'total_bytes': multirow_mock_collect.counter * 300}
    ]
    print('Collect: %s' % collect)
    multirow_mock_collect.counter += 4
    return collect

multirow_mock_collect.counter = 0


class DataCollectorTestCase(unittest.TestCase):
    def test_delta_calculation(self):
        collector = DataCollector(mock_collect, 'Col', 'Query name here')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [{'Col': 1, 'total_ms': 2, 'total_bytes': 200}])

    def test_delta_calculation_on_multiple_rows(self):
        collector = DataCollector(multirow_mock_collect, 'Col', 'Name of the query')
        self.assertEquals(collector.query_name, 'Name of the query')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [
            {'Col': 1, 'total_ms': 4, 'total_bytes': 400},
            {'Col': 2, 'total_ms': 8, 'total_bytes': 800},
            {'Col': 3, 'total_ms': 12, 'total_bytes': 1200}
        ])




if __name__ == '__main__':
    unittest.main()
