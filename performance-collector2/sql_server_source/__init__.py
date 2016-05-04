from collections import namedtuple

import pypyodbc

from query import Query
from source import Source
from sql_server_source.query_store import QueryStore


class SQLServerSource:
    def __init__(self):
        self.conn = None
        self.query_store = QueryStore()

    def get_sources(self, config):
        """
        Based on a single configuration ie one target database, construct all the Source instances
            which this source type have. Once for each available query.
        :param config: the config dictionary to use to initialize the source.
        :return: a list of Sources with their query set, connected and ready to go.
        """

        # TODO: it might be possible to reuse the same connection for all the queries.
        self.conn = SQLServerSource.get_connection(config)
        sources = []
        for sqlQry in self.query_store.queries:
            q = Query(key_column=sqlQry.key_column,
                      query_name=sqlQry.query_name,
                      mapping={},
                      non_data_fields=[],
                      get_data=lambda: self.get_records(sqlQry.query_text))
            s = Source('%s@%s' % (config['database'], config['host']), q)
            sources.append(s)
        return sources

    @staticmethod
    def get_connection(config):
        connection_string = 'Driver={SQL Server Native Client 11.0};Server=%s;Database=%s;Uid=%s;Pwd=%s;APP=PerfCol' \
                            % (config['host'], config['database'], config['username'], config['password'])

        # logger.info('Connecting to %s@%s.%s',  config['username'], config['host'], config['database'])
        connection = pypyodbc.connect(connection_string)
        # logger.info("Connected")

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
