import time
import logging
import asyncio


class Limiter:

    def __init__(self, interval, limit, start=0):
        self.interval = interval
        self.limit = limit
        self.count = start
        self.time = time.time()
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("Limiter({})".format(self.interval))

    async def check(self):
        curr_time = time.time()
        if (curr_time - self.time) > self.interval:
            self.time = curr_time
            self.count = 0
            self.logger.debug("Reset")
        elif self.count >= self.limit:
            delay = self.interval - (curr_time - self.time)
            self.logger.info("Delay: {}".format(delay))
            await asyncio.sleep(delay)
            await self.check()
        else:
            self.count += 1
            self.logger.debug("Count: {}".format(self.count))
