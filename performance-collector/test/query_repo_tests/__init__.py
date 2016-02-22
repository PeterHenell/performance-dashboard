import unittest

from config_manager import ConfigManager, Configuration
from database_access import DatabaseAccess
from query_store import QueryStore

manager = ConfigManager.from_file('test.ini')
config = manager.get_config('localhost.master')
db = DatabaseAccess(config)


class PerformanceStatisticsTestCase(unittest.TestCase):
    def test_wait_stats_available(self):
        waits = db.get_records(QueryStore.get_query_text('dm_os_wait_stats'))
        first = waits[0]
        self.assertTrue(first['wait_type'] is not None)
        self.assertTrue(first['waiting_tasks_count'] is not None)
        self.assertTrue(first['wait_time_ms'] is not None)
        self.assertTrue(first['max_wait_time_ms'] is not None)
        self.assertTrue(first['signal_wait_time_ms'] is not None)
        self.assertTrue(len(waits) > 100)

    def test_query_stats_available(self):
        recs = db.get_records(QueryStore.get_query_text('query_stats'))
        first = recs[0]
        self.assertTrue(first['statement_text'] is not None)
        self.assertTrue(first['execution_count'] is not None)
        self.assertTrue(first['total_elapsed_time'] is not None)
        self.assertTrue(first['total_rows'] is not None)
        self.assertTrue(first['total_logical_reads'] is not None)

    def test_get_procedure_stats_available(self):
        recs = db.get_records(QueryStore.get_query_text('dm_io_virtual_file_stats'))
        first = recs[0]
        self.assertTrue(first['num_of_reads'] is not None)

    def test_get_transformed_result(self):
        recs = [
            {'wait_type': 'Chilling',
             'waiting_tasks_count': 11,
             'wait_time_ms': 1,
             'max_wait_time_ms': 2,
             'signal_wait_time_ms': 33},
            ]

        transformed = QueryStore.transform_results_for(recs, 'dm_os_wait_stats')
        expected = [{'Chilling_waiting_tasks_count': 11,
                     'Chilling_wait_time_ms': 1,
                     'Chilling_max_wait_time_ms': 2,
                     'Chilling_signal_wait_time_ms': 33}]
        self.assertEquals(expected, transformed)

#
# {'wait_type': 'Chilling', 'waiting_tasks_count': 11, 'wait_time_ms': 1, 'max_wait_time_ms': 2, 'signal_wait_time_ms': 33},
#             {'wait_type': 'Relaxing', 'waiting_tasks_count': 22, 'wait_time_ms': 7, 'max_wait_time_ms': 3, 'signal_wait_time_ms': 55},
#             {'wait_type': 'Avoiding', 'waiting_tasks_count': 33, 'wait_time_ms': 9, 'max_wait_time_ms': 6, 'signal_wait_time_ms': 44}

if __name__ == '__main__':
    unittest.main()
