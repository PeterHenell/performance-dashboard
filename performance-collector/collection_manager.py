import threading
import time
from concurrent.futures import ThreadPoolExecutor as Pool
from timeit import default_timer as timer


def do_work(task):
    time.sleep(.1)  # pretend to do some lengthy work.
    return threading.current_thread().name, task  # return results

with Pool(max_workers=4) as executor:
    # submit work items to executor (in this case, just a number).
    start = timer()
    for name, task in executor.map(do_work, range(20)):
        print("%s %s" % (name, task))  # print results

# "Work" took .1 seconds per task.
# 20 tasks serially would be 2 seconds.
# With 4 threads should be about .5 seconds
print('time:', timer() - start)
