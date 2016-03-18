import time


class DataCollector:
    def __init__(self, collect_fun, data_key_col, query_name):
        self.collect = collect_fun
        self.cache = []
        self.data_key_col = data_key_col
        self.query_name = query_name

    @staticmethod
    def from_query(db, query_name, query_text, key_column):
        return DataCollector(lambda: db.get_records(query_text), key_column, query_name)

    @staticmethod
    def get_number_or_default(s, default):
        if not s:
            return default
        try:
            f = float(s)
            return f
        except ValueError:
            return default

    @staticmethod
    def is_number(s):
        if not s:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False

    def find_row_in_cache_by_key(self, key):
        if len(self.cache) > 0:
            for row in self.cache:
                if row[self.data_key_col] == key:
                    return row
        return None

    @staticmethod
    def calculate_delta_value(val, cached_row, key):
        if DataCollector.is_number(val):
            delta_value = val - cached_row[key]
            # If the new value for some reason is less than the old value (happens after restarts)
            if delta_value < 0:
                delta_value = 0
            return delta_value
        else:
            return None

    def calculate_row_delta(self, row, cached_row):
        row_delta = {}
        # Calculate only delta for each value which is not the key column
        non_key_data = {key: val for (key, val) in row.items() if key != self.data_key_col}
        for key, val in non_key_data.items():
            # delta is the diff between the new value and the previous value
            delta_value = DataCollector.calculate_delta_value(val, cached_row, key)
            # Only collect values that are meaningful
            if delta_value is not None and delta_value > 0:
                row_delta[key] = delta_value

        # Append the key and then row_delta to the result.
        # each call to get_delta will result in one row, but it might contain only the key
        row_delta[self.data_key_col] = row[self.data_key_col]
        return row_delta

    def get_delta(self):
        # TODO: Log logger.info(self.query_name starting collection...)
        data = self.collect()
        # TODO: Get data grouped by key column
        # if a is the key column and we have two measurements in two rows
        # [{a: key1, v1: 1 bytes},
        #  {a: key1, v2: 2ms } ]
        # Should result in one record {a: key1, v1: 1 bytes, v2: 2ms}
        # grouped_data = self.get_grouped(data, self.data_key_col)
        deltas = []
        for row in data:
            cached_row = self.find_row_in_cache_by_key(row[self.data_key_col])
            if cached_row is not None:
                row_delta = self.calculate_row_delta(row, cached_row)
                deltas.append(row_delta)
        self.cache = data
        return deltas

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.query_name == other.query_name

    def __ne__(self, other):
        return not self.__eq__(other)

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