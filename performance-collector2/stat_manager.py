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

        collector_config = config_manager.get_config('perf-collector')
        print('Starting collection, polling interval %s seconds' % collector_config['polling-interval-seconds'])

        delta_queue = ClosableQueue()
        elastic_queue = ClosableQueue()
        result_queue = ClosableQueue()

        source_manager = source_manager_cls(delta_queue)
        source_manager.load_sources(config_manager)

        elastic_api = elastic_api_cls.from_config_manager(config_manager)
        elastic_api.init_indexes_for(source_manager.sources)

        self.source_thread = TimedWorker(
                source_manager.process_all_sources,
                collector_config.get_int('polling-interval-seconds'))
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
        # TODO: can we stop gracefully?