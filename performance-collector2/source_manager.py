from config_manager import SourceType
from source import Source


class SourceManager:
    """
    Instantiates the sources for each of their queries

    contains list of sources []

    can help produce data.

    returns tuple (source, result) on get_deltas()
    """

    def __init__(self):
        self.sources = []

    @staticmethod
    def load_from_config_manager(config_manager):
        sm = SourceManager()
        source_types = config_manager.get_available_source_types()

        # For each collector type get the configured sources
        for source_type in source_types:
            sources = sm.get_source_for_class(source_type, config_manager)
            sm.add_sources(sources)
        return sm

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

    def get_data(self):
        for s in self.sources:
            data = s.get_records()
            assert type(data) is list, "Result from source must be a list of dict"
            yield (s, data)
