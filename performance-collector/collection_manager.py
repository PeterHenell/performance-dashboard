import threading
import time
from concurrent.futures import ThreadPoolExecutor as Pool
from timeit import default_timer as timer

from data_collector import DataCollector


class CollectionManager:

    def __init__(self, db, query_list):
        self.queries = query_list
        self.collectors = []
        self.db = db
        for name, q in self.queries.items():
            collector = DataCollector.from_query(self.db, name, q['sql_text'], q['key_col'])
            self.collectors.append(collector)

    def collect_data(self):
        result = []
        for c in self.collectors:
            delta = c.get_delta()
            if len(delta) > 0:
                result.append({c.query_name: delta})
        return result

    # def do_work(self, task):
    #     time.sleep(.1)  # pretend to do some lengthy work.
    #     return threading.current_thread().name, task  # return results
    #
    # with Pool(max_workers=4) as executor:
    #     # submit work items to executor (in this case, just a number).
    #     start = timer()
    #     for name, task in executor.map(do_work, range(20)):
    #         print("%s %s" % (name, task))  # print results
    #
    # # "Work" took .1 seconds per task.
    # # 20 tasks serially would be 2 seconds.
    # # With 4 threads should be about .5 seconds
    # print('time:', timer() - start)
