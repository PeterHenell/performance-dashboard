def nullif(val, compare_value):
    if val == compare_value:
        return None
    else:
        return val


def if_null(value, replacement_value):
    if value is not None:
        return value
    else:
        return replacement_value