from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
from query_store import QueryStore
import unittest


manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


def db_collector():
    return db.get_records(QueryStore.get_query_text('dm_io_virtual_file_stats'))


class MyTestCase(unittest.TestCase):
    def test_delta_calculation(self):
        collector = DataCollector(mock_collect, 'Col')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [{'Col': 1, 'total_ms': 2, 'total_bytes': 200}])



if __name__ == '__main__':
    unittest.main()
