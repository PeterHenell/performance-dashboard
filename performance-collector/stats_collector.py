from query_store import QueryStore
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

        # targets = self.config_manager.get_target_databases()
        # for db_name, db_config_key in targets:
        #     db_config = self.config_manager.get_config(db_config_key)
        #     #     TODO: Handle case when config key is not in config file
        #     for query_name, query_text in QueryStore.get_queries().items():
        #         self.initialize_elasticsearch(db_name, query_name)

    def run(self):
        # TODO: Logging instead of print
        from time import strftime
        timestamp = strftime("%Y-%m-%d %H:%M:%S")
        recorded_data = self.collection_manager.collect_data()
        print('Recorded: %s' % recorded_data)
        if len(recorded_data) > 0:
            enhanced_data = self.enhance(recorded_data, timestamp)
            print('Enhanced: %s' % enhanced_data)
            for records in enhanced_data:
                self.api.consume_to_index(records['delta'], self.db.db_name, records['query_name'])

    def initialize_elasticsearch(self, db_name, query_name):
        self.api.create_index(db_name, query_name)
        # TODO: If the data get jumbled in ES we might need to set the mapping for each query (doc type)
        # self.api.set_mapping()

    def enhance(self, recorded_data, timestamp):
        enhanced_data = []
        for query in recorded_data:
            for d in query['delta']:
                d['measured_at'] = timestamp
                enhanced_data.append({'query_name': query['query_name'], 'delta': d})
        return enhanced_data

    def cleanup(self):
        targets = self.config_manager.get_target_databases()
        for db_name, db_config_key in targets:
            db_config = self.config_manager.get_config(db_config_key)
            #     TODO: Handle case when config key is not in config file
            for query_name, query_text in QueryStore.get_queries().items():
                (hist, snap) = self.api.get_index_names(db_name, query_name)
                self.api.delete_index(hist)
                self.api.delete_index(snap)
