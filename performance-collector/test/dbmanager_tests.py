import unittest
from config_manager import ConfigManager
from database_access import DatabaseAccess

__author__ = 'peter.henell'

manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


class ConnectTestCase(unittest.TestCase):

    def test_should_connect_to_db(self):

        recs = db.get_records('select 1 as col')
        self.assertEquals(len(recs), 1)

    def test_should_get_record_by_name(self):
        recs = db.get_records('select 1 as col')
        self.assertEquals(recs[0]['col'], 1)

    def test_should_display_column_names(self):
        recs = db.get_records('select 1 as col, 2 as colb')

        for row in recs:
            self.assertEquals(row['col'], 1)
            self.assertEquals(row['colb'], 2)

    def test_should_serialize_to_json(self):
        actual = db.get_records('select 1 as col, 2 as colb')
        expected = [{'col': 1, 'colb': 2}]
        self.assertEquals(expected, actual)


if __name__ == '__main__':
    unittest.main()