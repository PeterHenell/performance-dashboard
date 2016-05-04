from source import Source
from query import Query


class MockSourceImpl:
    def get_sources(self, config):
        query = Query(key_column='a',
                      query_name='Mock',
                      mapping={},
                      non_data_fields=[],
                      get_data=lambda: ['Mocked result'])

        return [Source('', query)]
