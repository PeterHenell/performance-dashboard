class SourceManager:
    """
    Instantiates the sources for each of their queries

    contains list of sources []

    can help produce data.

    returns tuple (source, result) on get_deltas()
    """

    def __init__(self):
        self.sources = []

    def initialize_source(self, source_impl, config_manager):
        collector_types = config_manager.get_available_source_types()
        impl_instance = source_impl()

        # For each collector type get the configured sources
        for c in collector_types:
            configs = config_manager.get_config_for_source_name(c.name)
            for conf in configs:
                sources = impl_instance.get_sources(conf)
                self.sources.extend(sources)
