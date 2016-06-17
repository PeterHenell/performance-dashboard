from elasticsearch_api import ElasticsearchAPI
from source_manager import SourceManager
from stat_calculator import StatCalculator
from stoppable_worker import ClosableQueue, StoppableWorker, TimedWorker, LoggingWorker


class StatManager:
    """
    Uses source_manager to keep track of all the sources.
    Uses stat calculator to calculate deltas from the data_rows it got from the sources

    pushes delta to elasticsearch

    creates the timestamp which all the delta_rows will use.
    """

    def __init__(self, config_manager,
                 source_manager_cls=SourceManager,
                 elastic_api_cls=ElasticsearchAPI,
                 stat_calculator=StatCalculator):

        delta_queue = ClosableQueue()
        elastic_queue = ClosableQueue()
        result_queue = ClosableQueue()

        source_manager = source_manager_cls(delta_queue)
        source_manager.load_sources(config_manager)

        elastic_api = elastic_api_cls.from_config_manager(config_manager)
        elastic_api.init(source_manager.sources)

        # TODO: Read wait time from config instead of using hardcoded here
        # TODO: Use seconds instead of milliseconds
        self.source_thread = TimedWorker(source_manager.process_all_sources, 5000)
        self.delta_worker = StoppableWorker(stat_calculator.calculate_collection_delta, delta_queue, elastic_queue)
        self.elastic_worker = StoppableWorker(elastic_api.consume_collection, elastic_queue, result_queue)
        self.log_completed_worker = LoggingWorker(result_queue.get)

        self.threads = [
            self.source_thread, self.delta_worker, self.elastic_worker, self.log_completed_worker
        ]

    def run(self):
        for thread in self.threads:
            thread.start()

        self.source_thread.join()
        #
        # self.delta_worker.join()
        # self.delta_worker.stop()
        #
        # self.elastic_worker.join()
        # self.elastic_worker.stop()
