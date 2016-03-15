from config_manager import ConfigManager
from database_access import DatabaseAccess
from data_collector import DataCollector
from query_store import QueryStore
import unittest

manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


# def db_collector():
#     return db.get_records(QueryStore.get_query_text('dm_io_virtual_file_stats'))


def mock_collect():
    collect = [{'Col': 1, 'total_ms': mock_collect.counter, 'total_bytes': mock_collect.counter * 100}]
    # print('Collect: %s' % collect)
    mock_collect.counter += 2
    return collect


mock_collect.counter = 0


def multirow_mock_collect():
    collect = [
        {'Col': 1, 'total_ms': multirow_mock_collect.counter, 'total_bytes': multirow_mock_collect.counter * 100},
        {'Col': 2, 'total_ms': multirow_mock_collect.counter * 2, 'total_bytes': multirow_mock_collect.counter * 200},
        {'Col': 3, 'total_ms': multirow_mock_collect.counter * 3, 'total_bytes': multirow_mock_collect.counter * 300}
    ]
    # print('Collect: %s' % collect)
    multirow_mock_collect.counter += 4
    return collect


multirow_mock_collect.counter = 0


def pop_mock_collect():
    # print('Pop: %s' % pop_mock_collect.items)
    if len(pop_mock_collect.items) > 0:
        return pop_mock_collect.items.pop(0)
    return []


class DataCollectorTestCase(unittest.TestCase):
    def test_delta_calculation(self):
        collector = DataCollector(mock_collect, 'Col', 'Query name here')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [{'Col': 1, 'total_ms': 2, 'total_bytes': 200}])

    def test_delta_calculation_on_multiple_rows(self):
        collector = DataCollector(multirow_mock_collect, 'Col', 'Name of the query')
        self.assertEquals(collector.query_name, 'Name of the query')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [
            {'Col': 1, 'total_ms': 4, 'total_bytes': 400},
            {'Col': 2, 'total_ms': 8, 'total_bytes': 800},
            {'Col': 3, 'total_ms': 12, 'total_bytes': 1200}
        ])

    def test_zero_diff_should_give_no_result(self):
        pop_mock_collect.items = [[
            {
                'Col': 1,
                'total_ms': 100,
                'total_bytes': 333
            },
            {
                'Col': 2,
                'total_ms': 220,
                'total_bytes': 1000
            },
        ],
            [
                {
                    'Col': 1,
                    'total_ms': 100,
                    'total_bytes': 333
                },
                {
                    'Col': 2,
                    'total_ms': 456,
                    'total_bytes': 3456
                },
            ]]
        collector = DataCollector(pop_mock_collect, 'Col', 'Name of the query')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 236, 'total_bytes': 2456}
        ])

        delta3 = collector.get_delta()
        self.assertEquals(delta3, [])

        delta4 = collector.get_delta()
        self.assertEquals(delta4, [])

    def test_data_not_changing_should_be_zero_in_delta(self):
        pop_mock_collect.items = [[
            {
                'Col': 1,
                'total_ms': 100,
                'total_bytes': 333
            },
            {
                'Col': 2,
                'total_ms': 220,
                'total_bytes': 1000
            },
        ],
            # This one changes
            [
                {
                    'Col': 1,
                    'total_ms': 100,
                    'total_bytes': 333
                },
                {
                    'Col': 2,
                    'total_ms': 456,
                    'total_bytes': 3456
                },
            ],

            # This one is the same again
            [
                {
                    'Col': 1,
                    'total_ms': 100,
                    'total_bytes': 333
                },
                {
                    'Col': 2,
                    'total_ms': 456,
                    'total_bytes': 3456
                },
            ],
            # This one is the same again
            [
                {
                    'Col': 1,
                    'total_ms': 100,
                    'total_bytes': 333
                },
                {
                    'Col': 2,
                    'total_ms': 456,
                    'total_bytes': 3456
                },
            ]
        ]
        collector = DataCollector(pop_mock_collect, 'Col', 'Name of the query')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 236, 'total_bytes': 2456}
        ])

        delta3 = collector.get_delta()
        self.assertEquals(delta3, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 0, 'total_bytes': 0}
        ])

        delta4 = collector.get_delta()
        self.assertEquals(delta4, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 0, 'total_bytes': 0}
        ])

    def test_data_less_than_previous_should_be_zero_in_delta(self):
        pop_mock_collect.items = [[
            {
                'Col': 1,
                'total_ms': 100,
                'total_bytes': 333
            },
            {
                'Col': 2,
                'total_ms': 220,
                'total_bytes': 1000
            },
        ],
            # This one changes
            [
                {
                    'Col': 1,
                    'total_ms': 100,
                    'total_bytes': 333
                },
                {
                    'Col': 2,
                    'total_ms': 456,
                    'total_bytes': 3456
                },
            ],

            # This one is less than previous
            [
                {
                    'Col': 1,
                    'total_ms': 25,
                    'total_bytes': 6
                },
                {
                    'Col': 2,
                    'total_ms': 30,
                    'total_bytes': 11
                },
            ],
            # This one is the same again
            [
                {
                    'Col': 1,
                    'total_ms': 25,
                    'total_bytes': 6
                },
                {
                    'Col': 2,
                    'total_ms': 30,
                    'total_bytes': 11
                },
            ]
        ]
        collector = DataCollector(pop_mock_collect, 'Col', 'Name of the query')

        delta1 = collector.get_delta()
        self.assertEquals(delta1, [])

        delta2 = collector.get_delta()
        self.assertEquals(delta2, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 236, 'total_bytes': 2456}
        ])

        delta3 = collector.get_delta()
        self.assertEquals(delta3, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 0, 'total_bytes': 0}
        ])

        delta4 = collector.get_delta()
        self.assertEquals(delta4, [
            {'Col': 1, 'total_ms': 0, 'total_bytes': 0},
            {'Col': 2, 'total_ms': 0, 'total_bytes': 0}
        ])


if __name__ == '__main__':
    unittest.main()
