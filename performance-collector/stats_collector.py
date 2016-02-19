from database_access import DatabaseAccess
from query_store import QueryStore
import logging
import datetime
from elastic_api import ElasticAPI

logger = logging.getLogger('perf-collector')


class StatCollector:

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.api = ElasticAPI(config_manager)

    def workflow(self, query_name, query_text, db_name, db_config):
        logger.info('Starting collection of %s from %s' % (query_name, db_name))
        logger.debug('Running %s' % query_text)

        db = DatabaseAccess(db_config)
        records = db.get_records(query_text)
        self.api.consume_to_index(records, db_name, query_name)

        # get result of q from db
        # get snapshot of q from snap_index
        # calculate delta of (snap, result)
        # store (snap, db, q)
        # store (delta, timestamp, db, q)

    def initialize_elasticsearch(self, db_name, query_name):
        self.api.create_index(db_name, query_name)
        # self.api.set_mapping()

    def run(self):
        targets = self.config_manager.get_target_databases()
        for db_name, db_config_key in targets:
            db_config = self.config_manager.get_config(db_config_key)
            #     TODO: Handle case when config key is not in config file
            for query_name, query_text in QueryStore.get_queries().items():
                self.initialize_elasticsearch(db_name, query_name)
                self.workflow(query_name, query_text, db_name, db_config)


        # documents = [doc.jsondoc for doc in self.stats_producer.get_stat_statements()]
        # # for each pg_doc
        # # if pg_doc exists in snap
        # #      A = A + (S - PG)
        # #   else
        # #      A = PG
        # #   push to snapshot
        # #   calculate avgs
        # #   push to A
        # #   push to snap_history
        # matches = api.match_documents_to_stored_snapshots(documents)
        # new_queries, to_be_updated = api.split_to_new_and_existing(documents, matches)
        # to_be_updated = api.calculate_new_values(to_be_updated)
        # api.push_to_index(new_queries)
        # api.push_to_index(to_be_updated)
        # api.consume_snapshots(documents)
        # api.take_histogram_snapshot(datetime.now())

    def cleanup(self):
        targets = self.config_manager.get_target_databases()
        for db_name, db_config_key in targets:
            db_config = self.config_manager.get_config(db_config_key)
            #     TODO: Handle case when config key is not in config file
            for query_name, query_text in QueryStore.get_queries().items():
                (hist, snap) = self.api.get_index_names(db_name, query_name)
                self.api.delete_index(hist)
                self.api.delete_index(snap)

    # def calculate_new_values(self, docs):
    #     calculated_docs = []
    #     for new_doc in docs:
    #         snapshot_doc = self.get_snapshot_of(new_doc)
    #         current_doc = self.get_current_of(new_doc)
    #
    #         if new_doc['query_text'] != snapshot_doc['_source']['query_text']:
    #             logger.fatal('Hash and id does not match: %s \n %s', new_doc, snapshot_doc)
    #
    #         for key in new_doc:
    #             pg_value = new_doc[key]
    #
    #             # Update numeric fields (only)
    #             if self.is_number(pg_value):
    #                 stored_total = self.get_value_or_zero(current_doc, key)
    #                 snapshot_value = self.get_value_or_zero(snapshot_doc, key)
    #
    #                 if snapshot_value > pg_value:
    #                     logger.warn('Update: %s: %s => %s (calls %s => %s) For query %s ...' %
    #                                 (key, snapshot_value, pg_value, snapshot_doc['_source']['calls'], new_doc['calls'],
    #                                  new_doc['query_text'][:200]))
    #                     # TODO: If value_in_pg < stored_total then new_value = stored_total + value_in_pg
    #
    #                 new_doc[key] = int(float(stored_total) + (float(pg_value) - float(snapshot_value)))
    #
    #         calculated_docs.append(new_doc)
    #     return calculated_docs