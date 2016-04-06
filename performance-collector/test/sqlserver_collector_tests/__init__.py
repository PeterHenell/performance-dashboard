import unittest

from sqlserver_collector import InvalidSQLCollectorException, SQLServerCollector


class FailDBMock:
    def get(self):
        return []


class GoodDBMock:
    def get_records(self):
        return []


class SQLCollectionManagerTestCase(unittest.TestCase):
    def test_should_raise_error_if_collector_is_bad(self):
        with self.assertRaises(InvalidSQLCollectorException) as context:
            mockdb = FailDBMock()
            SQLServerCollector.from_query(mockdb, '', '', '')

        self.assertTrue(context.exception is not None)

        mockDb = GoodDBMock()
        col = SQLServerCollector.from_query(mockDb, '', '', '')

        self.assertTrue(col)

