from data_collector import DataCollector


class InvalidSQLCollectorException(Exception):
    pass


class SQLServerCollector:
    @staticmethod
    def append_collectors_from_queries(collection_manager, db, query_dict):
        for name, q in query_dict.items():
            collector = SQLServerCollector.from_query(db, name, q['sql_text'], q['key_col'])
            collection_manager.append_collector(collector)

    @staticmethod
    def from_query(db, query_name, query_text, key_column):
        # TODO: Log error
        invert_op = getattr(db, "get_records", None)
        if not callable(invert_op):
            raise InvalidSQLCollectorException('db does not have get_records method: %s' % db)

        return DataCollector(lambda: db.get_records(query_text), key_column, query_name)
