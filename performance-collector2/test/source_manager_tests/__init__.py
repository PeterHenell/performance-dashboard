import unittest

from config_manager import ConfigManager, SourceType
from query import Query
from source import Source
from source_manager import SourceManager
from mock_source import MockSourceImpl

from stoppable_worker import StoppableWorker, ClosableQueue

config_manager = ConfigManager.from_file('test.ini')

query = Query(key_column='a',
              query_name='Mock',
              mapping={},
              non_data_fields=[],
              get_data=lambda: ['Mocked result'])


class SourceManagerTests(unittest.TestCase):
    def test_should_get_sources_from_type(self):
        delta_queue = ClosableQueue()
        sm = SourceManager.from_config_manager(config_manager, delta_queue)

        mock = SourceType('MockSourceImpl', MockSourceImpl)
        sources = sm.get_source_for_class(mock, config_manager)
        self.assertEquals(len(sources), 1)

    def test_should_add_source(self):
        delta_queue = ClosableQueue()
        sm = SourceManager.from_config_manager(config_manager, delta_queue)

        source = Source('Fake', query)
        self.assertEquals(len(sm.sources), 1)
        sm.add_sources([source])
        self.assertEquals(len(sm.sources), 2)

    def test_should_load_from_config_manager(self):
        delta_queue = ClosableQueue()
        sm = SourceManager.from_config_manager(config_manager, delta_queue)

        self.assertTrue(len(sm.sources) == 1)

    def test_should_load_sql_server_source(self):
        manager = ConfigManager.from_file('sql_server.ini')
        delta_queue = ClosableQueue()
        sm = SourceManager.from_config_manager(manager, delta_queue)

        self.assertTrue(len(sm.sources) > 1)

    def test_should_get_result_from_source(self):
        manager = ConfigManager.from_file('test.ini')

        MockSourceImpl.records.clear()
        MockSourceImpl.records.append({'a': 10})
        delta_queue = ClosableQueue()
        sm = SourceManager.from_config_manager(config_manager, delta_queue)

        sm.process_all_sources()
        self.assertEquals(delta_queue.get(), [{'a': 10}])

    def test_should_run_in_worker(self):
        delta_queue = ClosableQueue()
        source_manager = SourceManager.from_config_manager(config_manager, delta_queue)

        MockSourceImpl.records.clear()
        MockSourceImpl.records.append({'a': 10})

        source_manager.process_all_sources()

        self.assertEquals(len(delta_queue), 1)




if __name__ == '__main__':
    unittest.main()
