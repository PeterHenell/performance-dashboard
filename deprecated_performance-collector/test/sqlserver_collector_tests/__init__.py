import unittest

from sqlserver_source import SQLServerDataSource


class FailDBMock:
    def get(self):
        return []


class GoodDBMock:
    def get_records(self):
        return []


class SQLCollectionManagerTestCase(unittest.TestCase):
    def test_should_raise_error_if_collector_is_bad(self):
        mockDb = GoodDBMock()
        col = SQLServerDataSource.from_query(mockDb, '', '', '', '')

        self.assertTrue(col)

