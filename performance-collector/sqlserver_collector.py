from data_collector import DataCollector
from database_access import DatabaseAccess
from query_store import QueryStore


class InvalidSQLCollectorException(Exception):
    pass


class SQLServerCollector:

    def __init__(self, db_class=DatabaseAccess, query_dict=QueryStore.queries):
        self.db_class = db_class
        self.query_dict = query_dict

    def get_collectors(self, collector_name, config):
        db = self.db_class(config)
        # TODO: Log error
        invert_op = getattr(db, "get_records", None)
        if not callable(invert_op):
            raise InvalidSQLCollectorException('db does not have get_records method: %s' % db)

        collectors = []
        for name, q in self.query_dict.items():
            collector = SQLServerCollector.from_query(db, name, q['sql_text'], q['key_col'], collector_name)
            collectors.append(collector)
        return collectors

    @staticmethod
    def from_query(db, query_name, query_text, key_column, collector_name):
        return DataCollector(lambda: db.get_records(query_text), key_column, query_name, collector_name)
