from source_manager import SourceManager
from stat_calculator import StatCalculator


class StatManager:
    """
    Uses source_manager to keep track of all the sources.
    Uses stat calculator to calculate deltas from the data_rows it got from the sources

    pushes delta to elasticsearch

    creates the timestamp which all the delta_rows will use.
    """

    def __init__(self, source_manager, elastic_api, StatCalcClass=StatCalculator):
        assert type(source_manager) is SourceManager

        self.source_manager = source_manager
        self.elastic_api = elastic_api
        self.statCalcClass = StatCalcClass


    def get_stats(self):
        data = self.source_manager.get_data()