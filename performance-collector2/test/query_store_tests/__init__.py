import unittest

from sql_server_source.query_store import QueryStore


class QueryStoreTests(unittest.TestCase):

    def test_get_queries_from_store(self):
        qs = QueryStore()
        self.assertTrue(len(qs.queries) > 1)
        qry = qs.queries[0]
        self.assertIsNotNone(qry.query_name)
        self.assertIsNotNone(qry.query_text)
        self.assertIsNotNone(qry.key_column)

if __name__ == '__main__':
    unittest.main()