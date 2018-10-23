import requests
import logging
from limiter import Limiter


class Requester:

    CC_DATA_API = "https://min-api.cryptocompare.com/data/{}"
    CC_STAT_API = "https://min-api.cryptocompare.com/stats/{}"
    APP_NAME = "InfluxDB Logger"
    logger = logging.getLogger("Requester")

    @classmethod
    def rate_limits(cls):
        result = requests.get(cls.CC_STAT_API.format("rate/limit"))
        return result.json()

    @classmethod
    def multi_price(cls, from_currencies, to_currencies, exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": cls.APP_NAME,
        }
        cls.logger.info("Simple: {} -> {}".format(from_currencies, to_currencies))
        try:
            result = requests.get(cls.CC_DATA_API.format("pricemulti"), params=payload)
            response = result.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                response = {}
                cls.logger.warning("Failed Fetching Data")
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response

    @classmethod
    def multi_price_full(cls, from_currencies, to_currencies, exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": cls.APP_NAME,
        }
        cls.logger.info("Full: {} -> {}".format(from_currencies, to_currencies))
        result = requests.get(cls.CC_DATA_API.format("pricemultifull"), params=payload)
        response = result.json()
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response.get("RAW", {})

    @classmethod
    def history_minute(cls, from_currencies, to_currencies, exchange=None, from_time=0):
        limits = cls.rate_limits()
        seclimiter = Limiter(1, 15, limits["Second"]["CallsMade"]["Histo"])
        minlimiter = Limiter(60, 300, limits["Minute"]["CallsMade"]["Histo"])
        hourlimiter = Limiter(3600, 8000, limits["Hour"]["CallsMade"]["Histo"])
        for from_currency in from_currencies:
            for to_currency in to_currencies:
                if from_currency == to_currency:
                    continue
                ret_data = []
                to_time = None
                try:
                    while True:
                        seclimiter.check()
                        minlimiter.check()
                        hourlimiter.check()
                        run = cls._history_minute(from_currency, to_currency, exchange, limit=2000, to_time=to_time)
                        ret_data = run + ret_data
                        to_time = run[0]["time"]
                        if len(run) < 2000 or to_time < from_time:
                            break
                except ValueError:
                    pass
                yield (from_currency, to_currency), ret_data

    @classmethod
    def _history_minute(cls, from_currency, to_currency, exchange=None, aggregate=None, limit=None, to_time=None):
        payload = {
            "fsym": from_currency,
            "tsym": to_currency,
            "e": exchange,
            "aggregate": aggregate,
            "limit": limit,
            "toTs": to_time,
            "extraParams": cls.APP_NAME,
        }
        cls.logger.info("History: {} -> {} ({})".format(from_currency, to_currency, to_time))
        result = requests.get(cls.CC_DATA_API.format("histominute"), params=payload)
        response = result.json()
        if response.get("Response") == "Error":
            raise ValueError(response["Message"])
        return response["Data"]
