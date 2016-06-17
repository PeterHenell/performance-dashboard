import logging

from collection_manager import CollectionManager
from config_manager import ClassLoader
from elastic_api import ElasticAPI
from query_store import QueryStore

logger = logging.getLogger('perf-collector')


class StatCollector:
    # should initialize all the collectors based on the config
    # should collect data from all the collectors on run()
    # should enhance the data from the collectors to include a timestamp (same for all records)
    # should thread calls

    def __init__(self, es_api, collectors):
        self.api = es_api
        self.collection_manager = CollectionManager()
        self.collection_manager.extend_collectors(collectors)

    @staticmethod
    def from_config_manager(config_manager, es_class=ElasticAPI):
        """
        :param config_manager: where to read all the sources from.
        :param es_class: the type to use for Elasticsearch interactions
        :return: an initialized StatCollector with all the collectors configured in the config_manager
        """
        collector_types = config_manager.get_available_source_types()
        collectors = []
        # For each source type get the configured sources
        for source_type_name, source_type in collector_types:
            sources = config_manager.get_sources_for_source_type(source_type_name)
            for source_config_key in sources:
                instance = source_type()
                config = config_manager.get_config(source_config_key[1])
                collectors.extend(
                        instance.get_collectors(
                                source_type_name, config))

        es_api = es_class(config_manager)
        stat_collector = StatCollector(es_api, collectors)

        return stat_collector

    def run(self):
        # TODO: Logging instead of print
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        recorded_data = self.collection_manager.collect_data()
        if len(recorded_data) > 0:
            # recorded data is list of lists{of documents}
            for records in recorded_data:
                self.api.consume_to_index(records, 'temporary_index_name', records['query_name'], timestamp)

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
