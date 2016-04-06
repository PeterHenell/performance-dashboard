import threading
import time
from concurrent.futures import ThreadPoolExecutor as Pool
from timeit import default_timer as timer

from data_collector import DataCollector





class CollectionManager:

    def __init__(self):
        self.collectors = []

    def append_collector(self, collector):
        self.collectors.append(collector)

    def collect_data(self):
        result = []
        for c in self.collectors:
            delta = c.get_delta()
            if len(delta) > 0:
                result.append({'query_name': c.query_name, 'delta': delta})
        return result
