import time


class DataCollector:
    def __init__(self, collect, data_key_col):
        self.collect = collect
        self.cache = []
        self.data_key_col = data_key_col

    def find_row_in_cache_by_key(self, key):
        if len(self.cache) > 0:
            for row in self.cache:
                if row[self.data_key_col] == key:
                    return row
        return None

    def get_delta(self):
        data = self.collect()
        # grouped_data = self.get_grouped(data, self.data_key_col)
        deltas = []
        for row in data:
            cached_row = self.find_row_in_cache_by_key(row[self.data_key_col])
            if cached_row is not None:
                row_delta = {}
                non_key_data = {key: val for (key, val) in row.items() if key != self.data_key_col}
                row_delta[self.data_key_col] = row[self.data_key_col]
                for key, val in non_key_data.items():
                    row_delta[key] = val - cached_row[key]
                deltas.append(row_delta)
        self.cache = data
        return deltas


# import threading
# from queue import Queue
# import time
#
# # lock to serialize console output
# lock = threading.Lock()
#
# def do_work(item):
#     time.sleep(.1) # pretend to do some lengthy work.
#     # Make sure the whole print completes or threads can mix up output in one line.
#     with lock:
#         print(threading.current_thread().name,item)
#
# # The worker thread pulls an item from the queue and processes it
# def worker():
#     while True:
#         item = q.get()
#         do_work(item)
#         q.task_done()
#
# # Create the queue and thread pool.
# q = Queue()
# for i in range(4):
#      t = threading.Thread(target=worker)
#      t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
#      t.start()
#
# # stuff work items on the queue (in this case, just a number).
# start = time.perf_counter()
# for item in range(20):
#     q.put(item)
#
# q.join()       # block until all tasks are done
#
# # "Work" took .1 seconds per task.
# # 20 tasks serially would be 2 seconds.
# # With 4 threads should be about .5 seconds (contrived because non-CPU intensive "work")
# print('time:',time.perf_counter() - start)