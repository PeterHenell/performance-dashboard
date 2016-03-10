from query_store import QueryStore
from elastic_api import ElasticAPI
from database_access import DatabaseAccess
from collection_manager import CollectionManager
import logging

logger = logging.getLogger('perf-collector')


class StatCollector:
    # should initialize all the collectors based on the config
    # should collect data from all the collectors on run()
    # should enhance the data from the collectors to include a timestamp (same for all records)
    # should thread calls
    def __init__(self, es_api, db_api, queries):

        self.api = es_api
        self.db = db_api

        self.collection_manager = CollectionManager(self.db, queries)
        # for query_name, query_text in queries.items():
        #         self.initialize_elasticsearch(db_name, query_name)

    @staticmethod
    def from_config_manager(config_manager, queries, es_class=ElasticAPI, db_class=DatabaseAccess):
        stat_collectors = []
        targets = config_manager.get_target_databases()
        es_api = es_class(config_manager)
        for db_name, db_config_key in targets:
            #     TODO: Handle case when config key is not in config file
            db_config = config_manager.get_config(db_config_key)
            db = db_class(db_config)
            sc = StatCollector(es_api, db, queries)
            stat_collectors.append(sc)
        return stat_collectors

    def run(self):
        # TODO: Logging instead of print
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        recorded_data = self.collection_manager.collect_data()
        if len(recorded_data) > 0:
            # enhanced_data = self.enhance(recorded_data, timestamp)
            # print('Enhanced: %s' % enhanced_data)
            for records in recorded_data:
                self.api.consume_to_index(records, self.db.db_name, records['query_name'], timestamp)
            # self.api.consume_to_index(enhanced_data, self.db.db_name, enhanced_data[0]['query_name'])

    def initialize_elasticsearch(self, index_name, query_name):
        self.api.create_index(index_name)
        self.api.set_mapping(index_name, query_name)

    def cleanup(self, config_manager):
        targets = config_manager.get_target_databases()
        for db_name, db_config_key in targets:
            #     TODO: Handle case when config key is not in config file
            a_db_name = config_manager.get_config(db_config_key)['database']
            for query_name, query_text in QueryStore.queries.items():
                index_name = self.api.get_index_names(a_db_name, query_name)
                self.api.delete_index(index_name)
                self.initialize_elasticsearch(index_name, query_name)
