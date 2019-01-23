from limiter import Limiter
from functools import partial
import requests
import logging
import asyncio


class CryptoCompare:

    CC_DATA_API = "https://min-api.cryptocompare.com/data/{}"
    CC_STAT_API = "https://min-api.cryptocompare.com/stats/{}"
    APP_NAME = "InfluxDB Logger"

    TYPE_MAP = {
        "TYPE": str,
        "MARKET": str,
        "FROMSYMBOL": str,
        "TOSYMBOL": str,
        "FLAGS": str,
        "PRICE": float,
        "LASTUPDATE": int,
        "LASTVOLUME": float,
        "LASTVOLUMETO": float,
        "LASTTRADEID": str,
        "VOLUMEDAY": float,
        "VOLUMEDAYTO": float,
        "VOLUME24HOUR": float,
        "VOLUME24HOURTO": float,
        "OPENDAY": float,
        "HIGHDAY": float,
        "LOWDAY": float,
        "OPEN24HOUR": float,
        "HIGH24HOUR": float,
        "LOW24HOUR": float,
        "LASTMARKET": str,
        "VOLUMEHOUR": float,
        "VOLUMEHOURTO": float,
        "OPENHOUR": float,
        "HIGHHOUR": float,
        "LOWHOUR": float,
        "CHANGE24HOUR": float,
        "CHANGEPCT24HOUR": float,
        "CHANGEDAY": float,
        "CHANGEPCTDAY": float,
        "SUPPLY": float,
        "MKTCAP": float,
        "TOTALVOLUME24H": float,
        "TOTALVOLUME24HTO": float,
        "IMAGEURL": str,

        "time": int,
        "close": float,
        "high": float,
        "low": float,
        "open": float,
        "volumefrom": float,
        "volumeto": float,
    }

    def __init__(self, key):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.key = key
        self.exchanges = self.get_exchanges()
        self.limits = self.rate_limits()
        second_made = self.limits["calls_made"]["second"]
        minute_made = self.limits["calls_made"]["minute"]
        hour_made = self.limits["calls_made"]["hour"]
        self.seclimiter = Limiter(1, self.limits["calls_left"]["second"] + second_made, second_made)
        self.minlimiter = Limiter(60, self.limits["calls_left"]["minute"] + minute_made, minute_made)
        self.hourlimiter = Limiter(3600, self.limits["calls_left"]["hour"] + hour_made, hour_made)
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
        ret = response.get("RAW", {})
        for from_currency, from_data in ret.items():
            for to_currency, to_data in from_data.items():
                currency_data = {}
                for key, val in to_data.items():
                    try:
                        val_type = self.TYPE_MAP[key]
                    except KeyError:
                        import pdb; pdb.set_trace()
                        self.logger.warning("Missing Key: {}".format(key))
                        val_type = str
                    currency_data[key] = val_type(val)
                ret[from_currency][to_currency] = currency_data
        return ret, exchange

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
        data = []
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
                data = run + data
                to_time = data[0]["time"]
                if len(run) < 2000 or to_time < from_time:
                    break
        except ValueError:
            pass
        ret_data = []
        for datapoint in data:
            ret_datapoint = {}
            for key, val in datapoint.items():
                try:
                    val_type = self.TYPE_MAP[key]
                except KeyError:
                    self.logger.warning("Missing Key: {}".format(key))
                    val_type = str
                ret_datapoint[key] = val_type(val)
            ret_data.append(ret_datapoint)
        return from_currency, to_currency, exchange, ret_data

    def _send_msg(self, url, params=None):
        if params is None:
            params = {}
        params["extraParams"] = self.APP_NAME
        try:
            result = requests.get(url, params=params, headers={'authorization': "Apikey {}".format(self.key)})
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
