from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
import unittest


manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


def mock_collect():
    print('Collect')
    return [{'Col': 1, 'Colb': 2}]


def mock_transform(data):
    return data


def mock_initialize():
    pass


class MyTestCase(unittest.TestCase):
    def test_something(self):
        DataCollector(mock_collect, mock_transform, mock_initialize)
        self.assertEqual(True, False, 'Not done yet')


if __name__ == '__main__':
    unittest.main()
