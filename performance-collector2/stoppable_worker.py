from queue import Queue
from threading import Thread

import time


class ClosableQueue(Queue):
    SENTINEL = object()

    def close(self):
        self.put(self.SENTINEL)

    def __iter__(self):
        while True:
            item = self.get()
            try:
                if item is self.SENTINEL:
                    return
                yield item
            finally:
                self.task_done()

    def __len__(self):
        return self.qsize()


class StoppableWorker(Thread):

    def __init__(self, func, in_queue, out_queue):
        super().__init__()
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.func = func

    def run(self):
        for item in self.in_queue:
            result = self.func(item)
            self.out_queue.put(result)

    def stop(self):
        self.in_queue.close()


class TimedWorker(Thread):

    def __init__(self, func, delay_between_processing):
        super().__init__()
        self.func = func
        self.delay = delay_between_processing
        self.is_stopped = False

    def run(self):
        while not self.is_stopped:
            self.func()
            time.sleep(self.delay)

    def stop(self):
        self.is_stopped = True


class LoggingWorker(Thread):

    def __init__(self, func, in_queue):
        super().__init__()
        self.func = func
        self.in_queue = in_queue

    def run(self):
        for _ in self.in_queue:
            pass
            # just consume the result queue for now
            # print(self.func())
