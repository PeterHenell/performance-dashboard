import unittest

from config_manager import ConfigManager
from query import Query
from source import Source
from source_manager import SourceManager

config_manager = ConfigManager.from_file('test.ini')


class MockSourceImpl:
    def get_sources(self, config):
        return Source()


class SourceManagerTests(unittest.TestCase):

    def test_instantiate(self):
        sm = SourceManager()
        sm.initialize_source(MockSourceImpl, config_manager)
        self.assertEquals(len(sm.sources), 1)


if __name__ == '__main__':
    unittest.main()
