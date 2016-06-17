import logging

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from source import Source
from stat_calculator import CalculatedData

logger = logging.getLogger('perf-collector')
handler = logging.StreamHandler()
logger.addHandler(handler)


class ElasticsearchAPI:
    """
    Each query will have its own index based on query name.
    index_name = query.name
    Doc type = query_name to make it possible to set mapping. Mapping is set per doc_type.

    All rows from a Query should look the same no matter the source.

    This makes all the data from all the servers in the same index.
        Comparable.
        Less indexes.
    """
    def __init__(self, host, port, user, password):
        logger.info("Connecting to ES %s..." % host)
        self.es = Elasticsearch(hosts=[
            {'host': host, 'port': port}, ])
        logger.debug(self.es.info())

    @staticmethod
    def from_config_manager(config_manager):
        config = config_manager.get_config('Elasticsearch')

        return ElasticsearchAPI(config['host'],
                                config['port'],
                                config['password'],
                                config['username'])

    def consume_collection(self, calculated_delta):
        assert type(calculated_delta) is CalculatedData

        query_name = calculated_delta.source.query.query_name
        db_name = calculated_delta.source.source_name
        docs = calculated_delta.delta_rows

        index_name = self.get_index_names(db_name)

        logger.debug('Pushing %s docs to index: %s' % (len(docs), index_name))
        print('Pushing %s docs to index: %s' % (len(docs), index_name))
        actions = []
        for doc in docs:
            action = {
                "_index": index_name,
                "_type": query_name + '_type',
                "_source": doc.to_dict(),
                }
            actions.append(action)
        # print(actions)
        helpers.bulk(self.es, actions)
        self.es.indices.refresh()

        return len(docs)

    def init(self, sources):
        for source in sources:
            self.init_source(source)

    def init_source(self, source):
        assert type(source) is Source
        query_name = source.query.query_name
        db_name = source.source_name

        index_name = self.get_index_names(
                db_name=db_name)

        self.create_index(index_name)
        self.set_mapping(index_name, source.query.query_name, source.query.mapping)

    def set_mapping(self, index_name, query_name, mapping):
        print('Setting mapping for %s' % index_name)
        print('todo: use query mapping instead of hardcoded mapping')
        mapping = {
            "properties": {
                "timestamp": {
                    "type": "date",
                    "format": "date_hour_minute_second"
                },
                "statement_text": {
                    "index": "not_analyzed",
                    "type": "string"
                },
                "file_id": {
                    "index": "not_analyzed",
                    "type": "string"
                }
            }
        }

        self.es.indices.put_mapping(
            index=index_name,
            doc_type=query_name + '_type',
            body=mapping)

    def delete_index(self, index_name):
        logger.info('Truncating data in index: %s' % index_name)
        self.es.indices.delete(index=index_name, ignore=404)

    def get_index_names(self, db_name):
        hist = 'hist-%s' % (db_name.replace('\\', '-'))
        return hist.lower()

    def create_index(self, index_name):
        print('Creating index %s' % index_name)
        self.es.indices.create(index_name, ignore=400)
