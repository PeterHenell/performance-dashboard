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

    def load_from_config_manager(self, config_manager):
        source_types = config_manager.get_available_source_types()

        # For each collector type get the configured sources
        for source_type in source_types:
            sources = self.get_source_for_class(source_type, config_manager)
            self.add_sources(sources)

    def add_sources(self, sources):
        for source in sources:
            assert type(source) is Source, "sources list must be list of Source"
            self.sources.append(source)

    def get_source_for_class(self, source_type, config_manager):
        assert type(source_type) is SourceType, "source_type must be of type SourceType"
        configs = config_manager.get_config_for_source_name(source_type.name)
        impl_instance = source_type.source_class()
        for conf in configs:
            sources = impl_instance.get_sources(conf)
            assert type(sources) is list, "return value from get_sources must be a list of Source"
            return sources
