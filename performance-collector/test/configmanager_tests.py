import unittest
from config_manager import ConfigManager, Configuration

__author__ = 'peter.henell'

manager = ConfigManager.from_file('test.ini')
config = Configuration(manager.get_config('Elasticsearch'))


class RecordFormatterTestCase(unittest.TestCase):

    def test_should_get_config_values(self):
        self.assertEqual('stat-statements', config.index)
        self.assertEqual('statement', config.doc_type)
        self.assertEqual('statement', config['doc_type'])

    def test_should_get_target_databases(self):
        actual = manager.get_target_databases()
        expected = [('database1', 'localhost.master'), ('database2', 'localhost.master'), ]
        self.assertEquals(expected, actual)


if __name__ == '__main__':
    unittest.main()