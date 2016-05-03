class StatManager:
    """
    Uses source_manager to keep track of all the sources.
    Uses stat calculator to calculate deltas from the data_rows it got from the sources

    pushes delta to elasticsearch

    creates the timestamp which all the delta_rows will use.
    """