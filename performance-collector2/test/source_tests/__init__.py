import unittest

from query import Query
from source import Source


class SourceTests(unittest.TestCase):

    def test_instantiate(self):
        q = Query(lambda: [{'t': 1}], 'q1', 't', {}, [])
        s = Source(source_name='src', query=q)

        result = s.get_records()
        self.assertEquals(result, [{'t': 1}])


if __name__ == '__main__':
    unittest.main()
