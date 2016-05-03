import unittest

from query import Query


class QueryTests(unittest.TestCase):

    def test_instantiate_query(self):
        q = Query(
                get_data=lambda: 'hej',
                query_name='q1',
                key_column='col1',
                mapping={'col1': 'not analyzed'},
                non_data_fields=[])

        self.assertEquals(q.key_column, 'col1')
        self.assertEquals(q.query_name, 'q1')
        self.assertEquals(q.mapping, {'col1': 'not analyzed'})
        self.assertEquals(q.non_data_fields, [])
        self.assertEquals(q.get_data(), 'hej')


if __name__ == '__main__':
    unittest.main()
