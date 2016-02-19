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

    # def calculate_stats(self, doc):
    #     logical_reads = if_utils.nullif(float(doc['shared_blks_hit']) + float(doc['shared_blks_read']), 0)
    #     if logical_reads:
    #         cache_miss_pcnt = int(100 - (100.0 * float(doc['shared_blks_hit']) / logical_reads))
    #         read_logical_blks = int(logical_reads)
    #     else:
    #         cache_miss_pcnt = None
    #         read_logical_blks = None
    #
    #     doc['avg_call_time_us'] = int((float(doc['total_time_ms']) / float(doc['calls']) * 1000))
    #     doc['rows_per_call'] = int((float(doc['rows']) / float(doc['calls'])))
    #     doc['cache_miss_pcnt'] = cache_miss_pcnt
    #     doc['read_logical_blks'] = read_logical_blks
    #
    #     return doc
    #
    # def push_to_index(self, docs, index_name):
    #     calculated = []
    #     for doc in docs:
    #         calculated_doc = self.calculate_stats(doc)
    #         calculated.append(calculated_doc)
    #
    #     self.consume_to_index(calculated, self.config['index'])

    def consume_to_index(self, docs, db_name, query_name):
        (hist_index_name, snap_index_name) = self.get_index_names(db_name, query_name)
        print('Pushing %s docs to index: %s, using id=query_hash' % (len(docs), hist_index_name))
        logger.debug('Pushing %s docs to index: %s, using id=query_hash' % (len(docs), hist_index_name))
        actions = []
        for doc in docs:
            print(doc)

            # TODO: Construct actual document
            action = {
                # "_id": doc['query_hash'],
                "_index": hist_index_name,
                "_type": 'doc_type',
                "_source": doc
                }
            actions.append(action)

        helpers.bulk(self.es, actions)
        self.es.indices.refresh()

    # def match_documents_to_stored_snapshots(self, docs):
    #     body = {"ids": [j['query_hash'] for j in docs]}
    #     return self.es.mget(index=self.config['snapshot_index'], doc_type=self.config['doc_type'], body=body)

    # def split_to_new_and_existing(self, new_docs, es_docs):
    #     new_queries = []
    #     existing_queries = []
    #
    #     for existing in es_docs['docs']:
    #         for n in new_docs:
    #             if n['query_hash'] == existing['_id']:
    #                 if existing['found'] is True:
    #                     existing_queries.append(n)
    #                 else:
    #                     new_queries.append(n)
    #
    #     logger.debug('New: %s' % len(new_queries))
    #     logger.debug('To be updated: %s ' % len(existing_queries))
    #
    #     return new_queries, existing_queries

    @staticmethod
    def is_number(s):
        if not s:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False

    def get_snapshot_of(self, doc):
        snap = self.es.get(
            index=self.config['snapshot_index'],
            doc_type=self.config['doc_type'],
            id=doc['query_hash'])
        return snap

    def get_current_of(self, doc):
        return self.es.get(
            index=self.config['index'],
            doc_type=self.config['doc_type'],
            id=doc['query_hash'])

    def get_value_or_zero(self, doc, key):
        try:
            value = doc['_source'][key]
            value = if_utils.if_null(value, 0)
            return value
        except KeyError:
            return 0



    def set_mapping(self, index_name):
        mapping = {
            "properties": {
                "username": {
                    "index": "not_analyzed",
                    "type": "string"
                },
                "query_text": {
                    "index": "not_analyzed",
                    "type": "string"
                }
            }
        }

        self.es.indices.put_mapping(
            index=index_name,
            doc_type='doc_type',
            body=mapping)

        # self.es.indices.put_mapping(
        #     index=index_name,
        #     doc_type='doc_type',
        #     body=mapping)
        #
        # self.es.indices.put_mapping(
        #     index=index_name,
        #     doc_type='doc_type',
        #     body=mapping)

    def delete_index(self, index_name):
        logger.info('Truncating data in index: %s' % self.config.index)
        self.es.indices.delete(index=index_name)

    def get_index_names(self, db_name, query_name):
        hist = 'hist-%s-%s' % (db_name, query_name)
        snap = 'snap-%s-%s' % (db_name, query_name)
        return hist, snap

    def create_index(self, db_name, query_name):
        (hist_index_name, snap_index_name) = self.get_index_names(db_name, query_name)
        self.es.indices.create(hist_index_name, ignore=400)
        self.es.indices.create(snap_index_name, ignore=400)
        self.set_mapping(hist_index_name)
        self.set_mapping(snap_index_name)
        #
        # self.es.indices.create(index=self.config['index'], ignore=400)
        # self.es.indices.create(index=self.config['snapshot_index'], ignore=400)
        # self.es.indices.create(index=self.config['histogram_index'], ignore=400)

    # def consume_snapshots(self, docs):
    #     self.consume_to_index(docs, self.config['snapshot_index'])
    #
    # def take_histogram_snapshot(self, timestamp):
    #     if not application_config['take_histogram_snapshots'].lower() in ['true', '1', 't', 'y', 'yes']:
    #         logger.info('Skipping histogram snapshot')
    #         return
    #
    #     logger.info('Storing histogram snapshot')
    #     search_query = {
    #         "query": {
    #             "match_all": {}
    #         }
    #     }
    #
    #     result = helpers.scan(client=self.es,
    #                           query=search_query,
    #                           scroll="2m",
    #                           index=self.config.index,
    #                           doc_type=self.config.doc_type)
    #     actions = []
    #     for res in result:
    #         doc = res['_source']
    #         doc['timestamp'] = timestamp
    #         doc['query_text'] = ''
    #
    #         action = {
    #             "_index": self.config['histogram_index'],
    #             "_type": self.config['doc_type'],
    #             "_source": doc
    #             }
    #         actions.append(action)
    #     logger.info('Snapshotting %s documents for %s', len(actions), timestamp)
    #     helpers.bulk(self.es, actions)