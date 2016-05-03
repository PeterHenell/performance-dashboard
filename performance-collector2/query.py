class Query:
    """
    Queries are a way for collectors to collect data. They are one way of getting data from the source.

    query_name - the name of the query
    key_column - the name of the key column in the result

    Does not produces anything but are a field of source.
    Only contain metadata about the query.
    Source can add functions to Query for collecting data from some kind of server.
    get_data_fun - the function or callable class to call in order for the query to collect data

    mapping - elasticsearch mapping specific for this query. If some of
                the fields from this query need to be mapped differently.
                Used during init of the indexes.
    non_data_fields = [] - Fields which should not be part of the delta calculations, instead be sent directly to es.
    """

    def __init__(self, get_data, query_name, key_column, mapping, non_data_fields):
        self.query_name = query_name
        self.key_column = key_column
        self.mapping = mapping
        self.non_data_fields = non_data_fields
        self.get_data = get_data
