from elasticsearch_api import ElasticsearchAPI
from source_manager import SourceManager
from stat_calculator import StatCalculator
from stoppable_worker import ClosableQueue, StoppableWorker


class StatManager:
    """
    Uses source_manager to keep track of all the sources.
    Uses stat calculator to calculate deltas from the data_rows it got from the sources

    pushes delta to elasticsearch

    creates the timestamp which all the delta_rows will use.
    """

    def __init__(self, source_manager_cls=SourceManager, elastic_api_cls=ElasticsearchAPI, StatCalcClass=StatCalculator):
        source_queue = ClosableQueue()
        delta_queue = ClosableQueue()
        elastic_queue = ClosableQueue()
        result_queue = ClosableQueue()

        source_manager = source_manager_cls()
        elastic_api = elastic_api_cls()

        source_worker = StoppableWorker(source_manager.get_data, source_queue, delta_queue)
        delta_worker = StoppableWorker(StatCalcClass.calculate_collection_delta, delta_queue, elastic_queue)
        elastic_worker = StoppableWorker(elastic_api.consume_collection, elastic_queue, result_queue)

        # self.source_manager = source_manager
        # self.elastic_api = elastic_api
        # self.statCalcClass = StatCalcClass


    def run(self):
        pass
