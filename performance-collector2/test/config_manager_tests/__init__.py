import unittest
from config_manager import ConfigManager, Configuration, ClassLoader

__author__ = 'peter.henell'

manager = ConfigManager.from_file('test.ini')
config = Configuration(manager.get_config('Elasticsearch'))


class SomeAmazingClass:
    def verify_class_have_been_loaded(self):
        return 'Yes, it works'


class ConfigManagerTestCase(unittest.TestCase):

    def test_should_get_available_source_types(self):
        source_types = manager.get_available_source_types()
        for c in source_types:
            print('%s: %s' % (c.name, c.source_class))
            c = c.source_class()
            self.assertTrue(c is not None)

    def test_should_get_sources_of_source_type(self):
        collector_types = manager.get_available_source_types()
        self.assertTrue(len(collector_types) > 0)

        # For each collector type get the configured sources
        for c in collector_types:
            configs = manager.get_config_for_source_name(c.name)
            self.assertTrue(len(configs) > 0)

    def test_should_load_class_from_string(self):
        cls = ClassLoader.get_class_from_text('Test class', 'config_manager_tests.SomeAmazingClass')
        inst = cls()
        res = inst.verify_class_have_been_loaded()
        self.assertEquals('Yes, it works', res)

    def test_should_get_multiple_sources_for_one_type(self):
        cm = ConfigManager.from_file('multiple.ini')
        configs = cm.get_config_for_source_name('MockSourceImpl')
        self.assertEquals(len(configs), 2)

if __name__ == '__main__':
    unittest.main()