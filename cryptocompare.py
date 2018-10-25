from limiter import Limiter
from functools import partial
import requests
import logging
import asyncio


class CryptoCompare:

    CC_DATA_API = "https://min-api.cryptocompare.com/data/{}"
    CC_STAT_API = "https://min-api.cryptocompare.com/stats/{}"
    APP_NAME = "InfluxDB Logger"

    def __init__(self):
        self.logger = logging.getLogger("CryptoCompare")
        self.limits = self.rate_limits()
        self.loop = asyncio.get_event_loop()
        self.seclimiter = Limiter(1, 50, self.limits["Second"]["CallsMade"]["Price"])
        self.minlimiter = Limiter(60, 2000, self.limits["Minute"]["CallsMade"]["Price"])
        self.hourlimiter = Limiter(3600, 100000, self.limits["Hour"]["CallsMade"]["Price"])

    def rate_limits(self):
        result = requests.get(self.CC_STAT_API.format("rate/limit"))
        return result.json()

    async def multi_price(self, from_currencies, to_currencies, exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": self.APP_NAME,
        }
        await self.seclimiter.check()
        await self.minlimiter.check()
        await self.hourlimiter.check()
        self.logger.info("Simple: {} -> {}".format(from_currencies, to_currencies))
        try:
            result = await self.loop.run_in_executor(
                None, partial(requests.get, self.CC_DATA_API.format("pricemulti"), params=payload))
            response = result.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                response = {}
                self.logger.warning("Failed Fetching Data")
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response

    async def multi_price_full(self, from_currencies, to_currencies, exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": self.APP_NAME,
        }
        await self.seclimiter.check()
        await self.minlimiter.check()
        await self.hourlimiter.check()
        self.logger.info("Full: {} -> {}".format(from_currencies, to_currencies))
        try:
            result = await self.loop.run_in_executor(
                None, partial(requests.get, self.CC_DATA_API.format("pricemultifull"), params=payload))
            response = result.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                response = {}
                self.logger.warning("Failed Fetching Data")
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response.get("RAW", {})

    async def history_minute(self, from_currencies, to_currencies, exchange=None, from_time=0):
        hseclimiter = Limiter(1, 15, self.limits["Second"]["CallsMade"]["Histo"])
        hminlimiter = Limiter(60, 300, self.limits["Minute"]["CallsMade"]["Histo"])
        hhourlimiter = Limiter(3600, 8000, self.limits["Hour"]["CallsMade"]["Histo"])
        for from_currency in from_currencies:
            for to_currency in to_currencies:
                if from_currency == to_currency:
                    continue
                ret_data = []
                to_time = None
                try:
                    while True:
                        await hseclimiter.check()
                        await hminlimiter.check()
                        await hhourlimiter.check()
                        run = await self._history_minute(from_currency, to_currency, exchange,
                                                         limit=2000, to_time=to_time)
                        if not run:
                            continue
                        ret_data = run + ret_data
                        to_time = ret_data[0]["time"]
                        if len(run) < 2000 or to_time < from_time:
                            break
                except ValueError:
                    pass
                yield from_currency, to_currency, ret_data

    async def _history_minute(self, from_currency, to_currency, exchange=None, aggregate=None, limit=None, to_time=None):
        payload = {
            "fsym": from_currency,
            "tsym": to_currency,
            "e": exchange,
            "aggregate": aggregate,
            "limit": limit,
            "toTs": to_time,
            "extraParams": self.APP_NAME,
        }
        self.logger.info("History: {} -> {} ({})".format(from_currency, to_currency, to_time))
        try:
            result = await self.loop.run_in_executor(
                None, partial(requests.get, self.CC_DATA_API.format("histominute"), params=payload))
            response = result.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                self.logger.warning("Failed Fetching Data")
                response = {"Data": []}
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response["Data"]