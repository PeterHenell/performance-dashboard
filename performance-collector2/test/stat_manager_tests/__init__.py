import unittest

from config_manager import ConfigManager
from source_manager import SourceManager
from stat_manager import StatManager


class ElasticMock:
    def __init__(self):
        self.mocked = True

    def init(self, sources):
        pass

    def consume_collection(self, delta_collection):
        pass


class StatManagerTests(unittest.TestCase):

    def test_instantiate(self):
        config_manager = ConfigManager.from_file('test.ini')
        statman = StatManager(config_manager, SourceManager, ElasticMock)

        # statman.run()