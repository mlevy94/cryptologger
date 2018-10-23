import time
import logging


class Limiter:

    def __init__(self, interval, limit, start=0):
        self.interval = interval
        self.limit = limit
        self.count = start
        self.time = time.time()
        self.logger = logging.getLogger("Limiter({})".format(self.interval))

    def check(self):
        curr_time = time.time()
        if (curr_time - self.time) > self.interval:
            self.time = curr_time
            self.count = 0
            self.logger.info("Reset")
        elif self.count >= self.limit:
            delay = self.interval - (curr_time - self.time)
            self.logger.info("Delay: {}".format(delay))
            time.sleep(delay)
            self.time = time.time()
            self.count = 0
        else:
            self.count += 1
            self.logger.info("Count: {}".format(self.count))
