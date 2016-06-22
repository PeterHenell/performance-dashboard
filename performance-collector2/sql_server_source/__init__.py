from collections import namedtuple

import pypyodbc

from query import Query
from source import Source
from sql_server_source.query_store import QueryStore


class SQLExecutor:
    def __init__(self, query_text, get_connection):
        self.query_text = query_text
        self.get_connection = get_connection

    def __call__(self, *args, **kwargs):
        return self.get_records(self.query_text)

    def get_records(self, sql):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)

                    column_names = [column[0] for column in cur.description]
                    rows = cur.fetchall()

                    DBRecord = namedtuple('DBRecord', column_names)
                    res = [r for r in map(DBRecord._make, rows)]

                    return [r._asdict() for r in res]

        except pypyodbc.Error as er:
            print('Error during SQLExecutor.get_records executing %s' % sql)
            print('The error was %s' % er)

        return []


class SQLServerSource:
    def __init__(self):
        self.query_store = QueryStore()
        self.config = None

    def get_sources(self, config):
        """
        Based on a single configuration ie one target database, construct all the Source instances
            which this source type have. Once for each available query.
        :param config: the config dictionary to use to initialize the source.
        :return: a list of Sources with their query set, connected and ready to go.
        """
        self.config = config
        sources = []
        for sqlQry in self.query_store.queries:
            executor = SQLExecutor(sqlQry.query_text, lambda: SQLServerSource.get_connection(self.config))
            q = Query(key_column=sqlQry.key_column,
                      query_name=sqlQry.query_name,
                      mapping={},
                      non_data_fields=[],
                      get_data=executor)
            # Source name is the host name for now
            s = Source(config['host'], q)
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
