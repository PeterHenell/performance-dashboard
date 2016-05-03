from query import Query


class Source:
    """
    Sources collects data using queries.
    They produce data_rows which are a snapshot of the values of the point of measurement

    Each source have a list of Queries they will use
    A source is instantiated for each database/server it have been configured to monitor for each query.
    For each configured_source:
        for each query in source:
            new Source(query, config)
    The reason to have this many source instances is because we want to have a cache for each query.

    query - reference to a Query.
    source_name = (database_name/server_name)
    cache

    get_records()
    """

    def __init__(self, source_name, query):
        self.source_name = source_name
        self.cache = DataRowCache()
        self.query = query

    def get_records(self):
        return self.query.get_data()


class DataRowCache:
    """
    Keeps cache of data_rows based on key
    """


class DataRows:
    """
    DTO class.

    list of dict data [{a: 10, c: 'peter'},]

    append the source_name to the result to make it possible to filter and group on
            the database_name or server_name whichever is used.
    """


class DeltaRows:
    """
    DTO class.

    Contain Calculated delta values.
    dict data {}
        contains:
            <value>_measured
            <value>_delta
            <value>_previous
            timestamp
    """
