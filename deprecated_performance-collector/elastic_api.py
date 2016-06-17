import logging
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import if_utils

logger = logging.getLogger('perf-collector')
handler = logging.StreamHandler()
logger.addHandler(handler)


class ElasticAPI:

    def __init__(self, config_manager):
        self.config = config_manager.get_config('Elasticsearch')
        logger.info("Connecting to ES %s..." % self.config['host'])
        self.es = Elasticsearch(hosts=[
            {'host': self.config['host'], 'port': self.config['port']}, ])
        logger.debug(self.es.info())

    def consume_to_index(self, docs, db_name, query_name, timestamp):
        index_name = self.get_index_names(db_name, query_name)
        # print('Pushing %s docs to index: %s, using id=query_hash' % (len(docs), hist_index_name))
        logger.debug('Pushing %s docs to index: %s' % (len(docs['delta']), index_name))
        print('Pushing %s docs to index: %s' % (len(docs['delta']), index_name))
        actions = []
        for doc in docs['delta']:
            doc['timestamp'] = timestamp
            action = {
                "_index": index_name,
                "_type": query_name + '_type',
                "_source": doc,
                }
            actions.append(action)
        # print(actions)
        helpers.bulk(self.es, actions)
        self.es.indices.refresh()

    def set_mapping(self, index_name, query_name):
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

    def get_index_names(self, db_name, query_name):
        hist = 'hist-%s-%s' % (db_name, query_name)
        return hist

    def create_index(self, index_name):
        self.es.indices.create(index_name, ignore=400)
