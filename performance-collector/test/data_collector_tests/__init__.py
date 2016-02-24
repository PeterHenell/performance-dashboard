from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
import unittest


manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


def mock_collect():
    collect = [{'Col': 1, 'total_ms': mock_collect.counter, 'total_bytes': mock_collect.counter * 100}]
    print('Collect: %s' % collect)
    mock_collect.counter += 2
    return collect

mock_collect.counter = 0


class MyTestCase(unittest.TestCase):
    def test_something(self):
        collector = DataCollector(mock_collect, 'Col')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [{'Col': 1, 'total_ms': 2, 'total_bytes': 200}])

        self.assertEqual(True, False, 'Not done yet')


if __name__ == '__main__':
    unittest.main()
