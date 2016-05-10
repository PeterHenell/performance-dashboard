import unittest

from source_manager import SourceManager
from stat_manager import StatManager


class ElasticMock:
    def __init__(self):
        self.mocked = True

    def consume_collection(self, delta_collection):
        pass


class StatManagerTests(unittest.TestCase):

    def test_instantiate(self):
        statman = StatManager(SourceManager, ElasticMock)

        statman.run()