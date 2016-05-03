class SQLServerSource:

    def get_sources(self, config):
        """
        Based on a single configuration ie one target database, construct all the Source instances
            which this source type have. Once for each available query.
        :param config: the config dictionary to use to initialize the source.
        :return: a list of Sources with their query set, connected and ready to go.
        """

        # TODO: it might be possible to reuse the same connection for all the queries.
        return []