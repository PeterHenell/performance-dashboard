import unittest

from config_manager import ConfigManager, Configuration
from database_access import DatabaseAccess
from query_store import QueryStore

manager = ConfigManager.from_file('../localhost.ini')
db = DatabaseAccess(manager)


class PerformanceStatisticsTestCase(unittest.TestCase):
    def test_wait_stats_available(self):
        waits = db.get_records(QueryStore.get_query_text('dm_os_wait_stats'))
        first = waits[0]
        self.assertTrue(first.wait_type is not None)
        self.assertTrue(first.waiting_tasks_count is not None)
        self.assertTrue(first.wait_time_ms is not None)
        self.assertTrue(first.max_wait_time_ms is not None)
        self.assertTrue(first.signal_wait_time_ms is not None)
        self.assertTrue(len(waits) > 100)

    def test_query_stats_available(self):
        recs = db.get_records(QueryStore.get_query_text('query_stats'))
        first = recs[0]
        self.assertTrue(first.sql_handle is not None)
        self.assertTrue(first.execution_count is not None)
        self.assertTrue(first.total_elapsed_time is not None)
        self.assertTrue(first.total_rows is not None)
        self.assertTrue(first.raw_sql is not None)

    def get_procedure_stats_available(self):
        recs = db.get_records(QueryStore.get_query_text('procedure_stats'))
        first = recs[0]
        self.assertTrue(first.object_id is not None)
        self.assertTrue(first.execution_count is not None)
        self.assertTrue(first.total_elapsed_time is not None)
        self.assertTrue(first.total_rows is not None)
        self.assertTrue(first.raw_sql is not None)


if __name__ == '__main__':
    unittest.main()
