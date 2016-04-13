import logging
from collections import namedtuple

import pypyodbc

logger = logging.getLogger('perf-collector')


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
