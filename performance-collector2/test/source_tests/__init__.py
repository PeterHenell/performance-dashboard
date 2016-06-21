import unittest

from query import Query
from source import Source, DeltaRow, DeltaField


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
        # to move the values into the hot part of the cache we need to push twice
        s.get_records()

        self.assertEquals(s.cache.get_row(1), {'t': 1, 'v': 55000})
        self.assertEquals(s.cache.get_row(2), {'t': 2, 'v': 11000})

    def test_should_serialize_delta_row_to_dict(self):
        dr = DeltaRow('a', 'timestamp')
        dr.add_field(DeltaField('a', measured='key value'))
        dr.add_field(DeltaField('b', measured=100, delta=50, previous=75))

        actual = dr.as_dict()
        self.assertEquals(actual, {'a_measured': 'key value', 'b_measured': 100, 'b_delta': 50, 'b_previous': 75,
                                   'timestamp': 'timestamp', 'key_col': 'key value'})


if __name__ == '__main__':
    unittest.main()
