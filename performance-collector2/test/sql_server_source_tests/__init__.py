import unittest

from config_manager import ConfigManager
from source_manager import SourceManager
from sql_server_source import SQLServerSource, QueryStore
from stoppable_worker import ClosableQueue

config_manager = ConfigManager.from_file('sql_server.ini')

query_store = QueryStore()


class SQLServerSourceTestCase(unittest.TestCase):

    def test_should_create_sql_server_source(self):
        config = config_manager.get_config('localhost.master')
        impl = SQLServerSource()
        sources = impl.get_sources(config)

        self.assertEquals(len(sources), len(query_store.queries))

        f = sources[0]
        self.assertIsNotNone(f.query)
        self.assertIsNotNone(f.source_name)
        self.assertIsNotNone(f.cache)

    def test_should_execute_query(self):
        delta_queue = ClosableQueue()
        sm = SourceManager.from_config_manager(config_manager, delta_queue)
        # for source in sm.sources:
        #     data = sm.get_data(source)
        #     self.assertTrue(len(data) > 0)
        #     print(data)
        #     print(source.query.query_name)
        sm.process_all_sources()
        self.assertEquals(len(delta_queue), 6)


if __name__ == '__main__':
    unittest.main()
