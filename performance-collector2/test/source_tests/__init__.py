import unittest

from query import Query
from source import Source


class SourceTests(unittest.TestCase):
    def test_instantiate(self):
        q = Query(lambda: [{'t': 1}], 'q1', 't', {}, [])
        s = Source(source_name='src', query=q)

        result = s.get_records()
        self.assertEquals(result, [{'t': 1}])

    def test_should_get_row_from_cache(self):
        q = Query(lambda: [{'t': 1, 'v': 55000},
                           {'t': 2, 'v': 11000}],
                  query_name='q1',
                  key_column='t',
                  mapping={},
                  non_data_fields=[])
        s = Source(source_name='src', query=q)

        s.get_records()

        self.assertEquals(s.cache.get_row(1), {'t': 1, 'v': 55000})
        self.assertEquals(s.cache.get_row(2), {'t': 2, 'v': 11000})


if __name__ == '__main__':
    unittest.main()
