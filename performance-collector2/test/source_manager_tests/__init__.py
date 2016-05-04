import unittest

from config_manager import ConfigManager, SourceType
from query import Query
from source import Source
from source_manager import SourceManager
from mock_source import MockSourceImpl

config_manager = ConfigManager.from_file('test.ini')

query = Query(key_column='a',
              query_name='Mock',
              mapping={},
              non_data_fields=[],
              get_data=lambda: ['Mocked result'])


class SourceManagerTests(unittest.TestCase):
    def test_should_get_sources_from_type(self):
        sm = SourceManager()
        mock = SourceType('MockSourceImpl', MockSourceImpl)
        sources = sm.get_source_for_class(mock, config_manager)
        self.assertEquals(len(sources), 1)

    def test_should_add_source(self):
        sm = SourceManager()
        source = Source('Fake', query)
        sm.add_sources([source])
        self.assertEquals(len(sm.sources), 1)

    def test_should_load_from_config_manager(self):
        sm = SourceManager.load_from_config_manager(config_manager)
        self.assertTrue(len(sm.sources) == 1)

    def test_should_load_sql_server_source(self):
        manager = ConfigManager.from_file('sql_server.ini')
        sm = SourceManager.load_from_config_manager(manager)
        self.assertTrue(len(sm.sources) > 1)

    def test_should_get_result_from_source(self):
        manager = ConfigManager.from_file('test.ini')

        MockSourceImpl.records.clear()
        MockSourceImpl.records.append({'a': 10})

        sm = SourceManager.load_from_config_manager(manager)

        for (source, records) in sm.get_data():
            self.assertEquals(records, [{'a': 10}])


if __name__ == '__main__':
    unittest.main()
