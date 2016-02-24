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

    def test_should_get_single_transformed_result(self):
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

    def test_should_get_list_transformed_result(self):
        recs = [
            {'wait_type': 'Chilling', 'waiting_tasks_count': 11, 'wait_time_ms': 1, 'max_wait_time_ms': 2,
             'signal_wait_time_ms': 33},
            {'wait_type': 'Relaxing', 'waiting_tasks_count': 22, 'wait_time_ms': 7, 'max_wait_time_ms': 3,
             'signal_wait_time_ms': 55},
            {'wait_type': 'Avoiding', 'waiting_tasks_count': 33, 'wait_time_ms': 9, 'max_wait_time_ms': 6,
             'signal_wait_time_ms': 44}
        ]

        transformed = QueryStore.transform_results_for(recs, 'dm_os_wait_stats')
        expected = [{'Chilling_waiting_tasks_count': 11,
                     'Chilling_wait_time_ms': 1,
                     'Chilling_max_wait_time_ms': 2,
                     'Chilling_signal_wait_time_ms': 33},
                    {'Relaxing_waiting_tasks_count': 22,
                     'Relaxing_wait_time_ms': 7,
                     'Relaxing_max_wait_time_ms': 3,
                     'Relaxing_signal_wait_time_ms': 55},
                    {'Avoiding_waiting_tasks_count': 33,
                     'Avoiding_wait_time_ms': 9,
                     'Avoiding_max_wait_time_ms': 6,
                     'Avoiding_signal_wait_time_ms': 44}]
        self.assertEquals(expected, transformed)

    def test_should_get_single_transformed_file_stat(self):
        #           dbname	    num_of_reads	num_of_bytes_read	io_stall_read_ms	io_stall_queued_read_ms	num_of_writes	num_of_bytes_written	io_stall_write_ms	io_stall_queued_write_ms	io_stall	size_on_disk_bytes
        #           PeterDB(1)	9170179	1964454543360	537681956	0	2759490	123165638656	21204520	0	558886476	1315582771200
        recs = [
            {'file_id': 'PeterDB(1)',
             'num_of_reads': 1,
             'num_of_bytes_read': 2,
             'io_stall_read_ms': 3,
             'io_stall_queued_read_ms': 4,
             'num_of_writes': 5,
             'num_of_bytes_written': 6,
             'io_stall_write_ms': 7,
             'io_stall_queued_write_ms': 8,
             'io_stall': 9,
             'size_on_disk_bytes': 10},
        ]

        transformed = QueryStore.transform_results_for(recs, 'dm_io_virtual_file_stats')
        expected = [{'PeterDB(1)_num_of_reads': 1,
                     'PeterDB(1)_num_of_bytes_read': 2,
                     'PeterDB(1)_io_stall_read_ms': 3,
                     'PeterDB(1)_io_stall_queued_read_ms': 4,
                     'PeterDB(1)_num_of_writes': 5,
                     'PeterDB(1)_num_of_bytes_written': 6,
                     'PeterDB(1)_io_stall_write_ms': 7,
                     'PeterDB(1)_io_stall_queued_write_ms': 8,
                     'PeterDB(1)_io_stall': 9,
                     'PeterDB(1)_size_on_disk_bytes': 10}]
        self.assertEquals(expected, transformed)


if __name__ == '__main__':
    unittest.main()
