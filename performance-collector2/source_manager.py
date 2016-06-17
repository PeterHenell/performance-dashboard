from datetime import time, datetime
from queue import Queue
from config_manager import SourceType
from source import Source, DataRowCache
from stoppable_worker import ClosableQueue


class SourceManager:
    """
    Instantiates the sources for each of their queries

    contains list of sources []

    can help produce data.

    returns tuple (source, result) on get_deltas()
    """
    def __init__(self, output_queue):
        self.sources = []
        assert type(output_queue) is ClosableQueue
        self.output_queue = output_queue

    @staticmethod
    def from_config_manager(config_manager, output_queue):
        sm = SourceManager(output_queue)
        sm.load_sources(config_manager)
        return sm

    def load_sources(self, config_manager):
        source_types = config_manager.get_available_source_types()

        # For each collector type get the configured sources
        for source_type in source_types:
            sources = self.get_source_for_class(source_type, config_manager)
            self.add_sources(sources)

    def add_sources(self, sources):
        for source in sources:
            assert type(source) is Source, "sources list must be list of Source"
            self.sources.append(source)

    @staticmethod
    def get_source_for_class(source_type, config_manager):
        assert type(source_type) is SourceType, "source_type must be of type SourceType"
        configs = config_manager.get_config_for_source_name(source_type.name)
        impl_instance = source_type.source_class()
        for conf in configs:
            sources = impl_instance.get_sources(conf)
            assert type(sources) is list, "return value from get_sources must be a list of Source"
            return sources

    def process_all_sources(self):
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for s in self.sources:
            data = s.get_records()
            # print('Data from get_records %s' % data)
            # print(data)
            assert type(data) is list, "Result from source must be a list of dict"
            collected = SourceData(data, s, timestamp)
            self.output_queue.put(collected)


class SourceData:
    def __init__(self, data_rows, source, timestamp):
        assert type(source) is Source
        assert type(data_rows) is list

        self.rows = data_rows
        self.source = source
        self.timestamp = timestamp
