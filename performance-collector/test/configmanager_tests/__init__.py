import unittest
from config_manager import ConfigManager, Configuration, ClassLoader

__author__ = 'peter.henell'

manager = ConfigManager.from_file('test.ini')
config = Configuration(manager.get_config('Elasticsearch'))


class SomeAmazingClass:
    def me(self):
        return 'Yes, it works'


class ConfigManagerTestCase(unittest.TestCase):

    def test_should_get_target_databases(self):
        actual = manager.get_target_databases()
        expected = [('database1', 'localhost.master'), ('database2', 'localhost.master'), ]
        self.assertEquals(expected, actual)

    def test_should_get_available_collectors(self):
        collector_types = manager.get_available_collectors()
        for name, col_type in collector_types:
            print('%s: %s' % (name, col_type))
            c = col_type()
            self.assertTrue(c is not None)

    def test_should_get_sources_of_collector_type(self):
        collector_types = manager.get_available_collectors()
        self.assertTrue(len(collector_types) > 0)

        # For each collector type get the configured sources
        for name, col_type in collector_types:
            sources = manager.get_sources_for_collector(name)
            self.assertTrue(len(sources) > 0)

    def test_should_load_class_from_string(self):
        cls = ClassLoader.get_class_from_text('Test class', 'configmanager_tests.SomeAmazingClass')
        inst = cls()
        res = inst.me()
        self.assertEquals('Yes, it works', res)


if __name__ == '__main__':
    unittest.main()