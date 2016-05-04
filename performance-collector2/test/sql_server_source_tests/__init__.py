import unittest

from config_manager import ConfigManager
from sql_server_source import SQLServerSource

config_manager = ConfigManager.from_file('sql_server.ini')


class SQLServerSourceTestCase(unittest.TestCase):

    def test_should_create_sql_server_source(self):
        config = config_manager.get_config('localhost.master')
        impl = SQLServerSource()
        sources = impl.get_sources(config)

        self.assertEquals(len(sources), 1)

        f = sources[0]
        self.assertIsNotNone(f.query)
        self.assertIsNotNone(f.source_name)
        self.assertIsNotNone(f.cache)


if __name__ == '__main__':
    unittest.main()