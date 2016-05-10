class ElasticsearchAPI:
    """
    Each query will have its own index based on query name.
    index_name = query.name
    Doc type = query_name to make it possible to set mapping. Mapping is set per doc_type.

    All rows from a Query should look the same no matter the source.

    This makes all the data from all the servers in the same index.
        Comparable.
        Less indexes.
    """


    def consume_collection(self, delta_collection):
        pass