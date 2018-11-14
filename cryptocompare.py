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
        self.logger = logging.getLogger(self.__class__.__name__)
        self.exchanges = self.get_exchanges()
        self.limits = self.rate_limits()
        self.seclimiter = Limiter(1, 50, self.limits["calls_made"]["second"])
        self.minlimiter = Limiter(60, 2000, self.limits["calls_made"]["minute"])
        self.hourlimiter = Limiter(3600, 100000, self.limits["calls_made"]["hour"])
        self.loop = asyncio.get_event_loop()

    def rate_limits(self):
        result = requests.get(self.CC_STAT_API.format("rate/limit"))
        return result.json()["Data"]

    async def coinlist(self):
        await self._check()
        return await self.loop.run_in_executor(None, self._send_msg, self.CC_DATA_API.format("all/coinlist"))

    def get_exchanges(self):
        return self._send_msg(self.CC_DATA_API.format("all/exchanges"))

    async def multi_price(self, from_currencies, to_currencies, exchange=None):
        await self._check()
        fsyms = ",".join(from_currencies)
        tsyms = ",".join(to_currencies)
        payload = {
            "fsyms": fsyms,
            "tsyms": tsyms,
            "e": exchange,
        }
        self.logger.info("Simple: [{}] {} -> {}".format(exchange, fsyms, tsyms))
        response = await self.loop.run_in_executor(
            None, partial(self._send_msg, self.CC_DATA_API.format("pricemulti"), params=payload))
        return response, exchange

    async def multi_price_full(self, from_currencies, to_currencies, exchange=None):
        await self._check()
        fsyms = ",".join(from_currencies)
        tsyms = ",".join(to_currencies)
        payload = {
            "fsyms": fsyms,
            "tsyms": tsyms,
            "e": exchange,
        }
        self.logger.info("Full: [{}] {} -> {}".format(exchange, fsyms, tsyms))
        response = await self.loop.run_in_executor(
            None, partial(self._send_msg, self.CC_DATA_API.format("pricemultifull"), params=payload))
        return response.get("RAW", {}), exchange

    async def history_minute(self, from_currencies, to_currencies, exchanges=None, from_time=0):
        futures = []
        for exchange in exchanges:
            if exchange in ["None", "CCCAGG", None]:
                exchange = "CCCAGG"
            elif exchange not in self.exchanges.keys():
                self.logger.warning("Invalid Exchange, Skipping: {}".format(exchange))
                continue
            for from_currency in from_currencies:
                for to_currency in to_currencies:
                    if from_currency == to_currency:
                        continue
                    try:
                        assert to_currency in self.exchanges[exchange][from_currency]
                    except (KeyError, AssertionError):
                        if exchange != "CCCAGG":
                            continue
                    futures.append(asyncio.ensure_future(self._history_minute(
                        from_currency, to_currency, exchange, limit=2000, from_time=from_time),
                        loop=self.loop,
                    ))
        return futures

    async def _history_minute(self, from_currency, to_currency, exchange=None, aggregate=None, limit=None, from_time=0):
        ret_data = []
        to_time = None
        try:
            while True:
                await self._check()
                payload = {
                    "fsym": from_currency,
                    "tsym": to_currency,
                    "e": exchange,
                    "aggregate": aggregate,
                    "limit": limit,
                    "toTs": to_time,
                }
                self.logger.info("Historic: [{}] {} -> {} ({})".format(exchange, from_currency, to_currency, to_time))
                response = await self.loop.run_in_executor(
                    None, partial(self._send_msg, self.CC_DATA_API.format("histominute"), params=payload))
                run = response.get("Data")
                if not run:
                    continue
                ret_data = run + ret_data
                to_time = ret_data[0]["time"]
                if len(run) < 2000 or to_time < from_time:
                    break
        except ValueError:
            pass
        return from_currency, to_currency, exchange, ret_data

    def _send_msg(self, url, params=None):
        if params is None:
            params = {}
        params["extraParams"] = self.APP_NAME
        try:
            result = requests.get(url, params=params)
            response = result.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                response = {}
                self.logger.warning("Failed Fetching Data")
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response

    async def _check(self):
        await self.seclimiter.check()
        await self.minlimiter.check()
        await self.hourlimiter.check()
