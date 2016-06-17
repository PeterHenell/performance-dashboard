from source import DeltaRow, DeltaField, Source
from source_manager import SourceData


class StatCalculator:
    """
    Calculates delta_rows from data_rows.

    caches for the sources are kept in the sources.

    @staticMethod
    get_deltas(rows, source)
    """

    @staticmethod
    def calculate_row_delta(row, cached_row, data_key_col, timestamp):
        assert type(row) is dict
        assert type(cached_row) is dict, "cached row must be dict even if empty"

        delta_row = DeltaRow(data_key_col, timestamp)

        # Calculate only delta for each value which is not the key column
        non_key_data = {key: val for (key, val) in row.items() if key != data_key_col}
        for field_name, measured_value in non_key_data.items():
            delta_field = StatCalculator.get_delta_field(cached_row, field_name, measured_value)
            delta_row.add_field(delta_field)

        # Append the key
        # each call to get_delta will result in one row, but it might contain only the key
        delta_row.add_field(DeltaField(field_name=data_key_col, measured=row[data_key_col]))
        return delta_row

    @staticmethod
    def get_delta_field(cached_row, field_name, measured_value):
        delta_field = DeltaField(field_name=field_name, measured=measured_value)
        if len(cached_row) > 0:
            prev_value = cached_row[field_name]
        else:
            prev_value = None

        # delta is the diff between the new value and the previous value
        (prev_value, delta_value) = StatCalculator.calculate_values(measured_value, prev_value)
        # Only collect measured values that are meaningful
        if delta_value is not None and delta_value > 0:
            delta_field.delta = delta_value
            delta_field.previous = prev_value
        return delta_field

    @staticmethod
    def calculate_values(val, prev_value):
        if StatCalculator.is_number(val) and StatCalculator.is_number(prev_value):
            delta_value = val - prev_value
            # If the new value for some reason is less than the old value (happens after sqlserver restarts etc)
            if delta_value < 0:
                delta_value = 0
            return prev_value, delta_value
        else:
            return None, None

    @staticmethod
    def calculate_collection_delta(source_data):
        assert type(source_data) is SourceData
        calculated = CalculatedData(source_data.source)
        for row in source_data.rows:
            assert type(row) is dict, "Each row in delta must be a dict"
            key_col = source_data.source.query.key_col
            row_key_value = row[key_col]
            cached_row = source_data.cache.get_row(row_key_value)

            calculated_delta_row = StatCalculator.calculate_row_delta(row, cached_row, key_col)
            calculated.append(calculated_delta_row)
        return calculated

    @staticmethod
    def is_number(s):
        if not s:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False


class CalculatedData:
    def __init__(self, source):
        assert type(source) is Source

        self.delta_rows = []
        self.source = source

    def append(self, delta_row):
        assert type(delta_row) is DeltaRow
        self.delta_rows.append(delta_row)

    def __len__(self):
        return len(self.delta_rows)
