import time
import logging
import asyncio


class Limiter:

    def __init__(self, interval, limit, start=0):
        self.interval = interval
        self.limit = limit
        self.count = start
        self.time = time.monotonic()
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("Limiter({})".format(self.interval))
        self.logger.info("Initialized! Limit: {} Start: {}".format(limit, start))

    async def check(self):
        curr_time = time.monotonic()
        if (curr_time - self.time) > self.interval:
            self.time = curr_time
            self.count = 0
            self.logger.debug("Reset")
        elif self.count >= self.limit:
            delay = round(self.interval - (curr_time - self.time) + 0.05, 1)
            self.logger.info("Delay: {}".format(delay))
            await asyncio.sleep(delay)  # slight extra delay to account for inaccuracies in sleep time
            await self.check()
        else:
            self.count += 1
            self.logger.debug("Count: {}".format(self.count))
