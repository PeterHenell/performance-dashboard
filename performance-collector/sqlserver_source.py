from data_collector import DataCollector
from query_store import QueryStore

import logging
from collections import namedtuple

import pypyodbc

logger = logging.getLogger('perf-collector')


class InvalidSQLCollectorException(Exception):
    pass


class DatabaseAccess:

    def __init__(self, config):
        self.conn = self.get_connection(config)

    @staticmethod
    def get_connection(config):
        connection_string = 'Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;Uid=%s;Pwd=%s;APP=PerfCol' \
                           % (config['host'], config['database'], config['username'], config['password'])

        logger.info('Connecting to %s@%s.%s',  config['username'], config['host'], config['database'])
        connection = pypyodbc.connect(connection_string)
        logger.info("Connected")

        return connection

    def get_records(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)

        column_names = [column[0] for column in cur.description]
        rows = cur.fetchall()
        cur.close()

        DBRecord = namedtuple('DBRecord', column_names)
        res = [r for r in map(DBRecord._make, rows)]

        return [r._asdict() for r in res]

    def execute(self, sql):
        crs = self.conn.cursor()
        crs.execute(sql)


class SQLServerDataSource:

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
            collector = SQLServerDataSource.from_query(db, name, q['sql_text'], q['key_col'], collector_name)
            collectors.append(collector)
        return collectors

    @staticmethod
    def from_query(db, query_name, query_text, key_column, collector_name):
        return DataCollector(lambda: db.get_records(query_text), key_column, query_name, collector_name)
