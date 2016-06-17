from source import Source
from query import Query
from collections import deque


class MockSourceImpl:
    def get_sources(self, config):
        query = Query(key_column='a',
                      query_name='Mock',
                      mapping={},
                      non_data_fields=[],
                      get_data=lambda: [MockSourceImpl.records.pop()])

        return [Source('', query)]

MockSourceImpl.records = deque([])
