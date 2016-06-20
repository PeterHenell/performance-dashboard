from collections import OrderedDict

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
        assert type(query) is Query
        self.source_name = source_name
        self.query = query
        self.cache = DataRowCache(query.key_column)

    def get_records(self):
        records = self.query.get_data()
        self.cache.cached_records = records
        return records


class DataRowCache:
    """
    Keeps cache of data_rows based on key
    """

    def __init__(self, data_key_column):
        """
        :param data_key_column: the name of the key in the result (ie DatabaseID)
        """
        self.cached_records = []
        self.data_key_column = data_key_column

    def get_row(self, key_value):
        """
        Get the cached record which have the data_key_column = key_value
        :param key_value:
        :return:
        """
        cached_row = self.find_row_by_key(key_value)
        assert type(cached_row) is OrderedDict or None or dict
        return cached_row

    def find_row_by_key(self, key):
        if len(self.cached_records) > 0:
            for row in self.cached_records:
                if row[self.data_key_column] == key:
                    return row
        return None


class DeltaRow:
    def __init__(self, key_column_name, timestamp):
        self.key_column_name = key_column_name
        self.timestamp = timestamp
        self.delta_fields = {}

    def add_field(self, delta_field):
        assert type(delta_field) is DeltaField
        self.delta_fields[delta_field.field_name] = delta_field

    def __getitem__(self, key):
        return self.delta_fields[key]

    def as_dict(self):
        d = {}
        # Add the key col with a fixed key_name
        # This will make it easier to group the data in kibana
        d['key_col'] = self.delta_fields[self.key_column_name]
        for name, field in self.delta_fields.items():
            d[name + '_measured'] = field.measured
            d['timestamp'] = self.timestamp
            if field.delta is not None:
                d[name + '_delta'] = field.delta
            if field.previous is not None:
                d[name + '_previous'] = field.previous
        return d

    def __repr__(self):
        return self.as_dict()


class DeltaField:
    """
    DTO class.

    Contain Calculated delta values.
    dict data {}
        contains:
            <value>_measured
            <value>_delta
            <value>_previous
    """

    def __init__(self, field_name, measured, delta=None, previous=None):
        self.field_name = field_name
        self.measured = measured
        self.delta = delta
        self.previous = previous

    def __repr__(self):
        return
