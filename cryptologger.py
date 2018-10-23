#!/usr/bin/env python
from argparse import ArgumentParser
import requests
import logging
import textwrap
import time


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
        result = requests.get(cls.CC_DATA_API.format("pricemulti"), params=payload)
        response = result.json()
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


class InfluxDB:

    MAX_POST = 5000

    def __init__(self, database, host="localhost", port=8086):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.database = database
        self.host = host
        self.port = port
        self.target = "http://{}:{}/{{}}".format(self.host, self.port)
        try:
            status = requests.post(self.target.format("query"), params={"q": "CREATE DATABASE {}".format(database)})
        except requests.exceptions.ConnectionError:
            raise ConnectionError("Failed connecting to InfluxDB at {}".format(self.target.format("")))
        status.raise_for_status()
        self.logger.info("Connection Established: {} ({})".format(self.database, self.target.format("")))

    @classmethod
    def make_price_pair(cls, fields, point_time=None):
        tags = {"to": fields.pop("TOSYMBOL")}
        fromsym = fields.pop("FROMSYMBOL")
        market = fields.pop("MARKET", None)
        if market is not None:
            tags["market"] = market
        tagstring = ",".join("{}={}".format(key, val) for key, val in tags.items())
        fieldstring = ",".join(map(cls._format_fields, fields.items()))
        if point_time is None:
            point_time = ""
        else:
            point_time = point_time * 1000000000
        return "{},{} {} {}".format(fromsym, tagstring, fieldstring, point_time)

    @staticmethod
    def _format_fields(pair):
        key, val = pair
        if isinstance(val, str):
            val = '"{}"'.format(val)
        elif isinstance(val, bool):
            val = str(val)
        return "{}={}".format(key, val)

    def post_data(self, data):
        if len(data) <= self.MAX_POST:
            self._post_data(data)
        else:
            for i in range(0, len(data), self.MAX_POST):
                self._post_data(data[i:i + self.MAX_POST])

    def _post_data(self, data):
        self.logger.info("Sending {} Data Points".format(len(data)))
        try:
            status = requests.post(
                self.target.format("write"), data="\n".join(data), headers={'Content-Type': 'application/octet-stream'},
                params={"db": self.database}
            )
            status.raise_for_status()
        except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
            self.logger.warning("Failed Sending Data")


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--single", action="store_true", help="Single run only")
    parser.add_argument("-i", "--interval", type=float, default=10.0,
                        help="Interval between queries in seconds (default: 10)")
    parser.add_argument("-o", "--host", default="localhost", help="InfluxDB host address (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=8086, help="InfluxDB port (default: 8086)")
    parser.add_argument("-d", "--database", default="crypto", help="InfluxDB database (default: crypto)")
    parser.add_argument("-f", "--from-symbols", type=str, nargs="*", default=["BTC", "BCH", "ETH"],
                        help="List of from symbols (default: BTC BCH ETH)")
    parser.add_argument("-t", "--to-symbols", type=str, nargs="*", default=["USD"],
                        help="List of to symbols (default: USD)")
    parser.add_argument("-e", "--exchange", type=str, default=None, help="Exchange to get values from")
    parser.add_argument("-m", "--simple", action="store_true", help="Get just the price of each pairing")
    parser.add_argument("-y", "--historic", action="store_true", help="Get last 7 days of data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s][%(name)-9s] %(message)s",
                        datefmt="%m-%d %H:%M:%S")
    logger = logging.getLogger("Main")

    logger.info(textwrap.dedent("""
    Configuration:
        -InfluxDB Server: {host}:{port}
        -InfluxDB Database: {database}
        -Interval: {interval}
        -From Symbols: {fromsyms}
        -To Symbols: {tosyms}
        -Exchange: {exchange}
        -Data: {data}""".format(
            host=args.host, port=args.port, database=args.database,
            interval="Single" if args.single else "Historic" if args.historic else "{} Seconds".format(args.interval),
            fromsyms=args.from_symbols, tosyms=args.to_symbols,
            exchange=args.exchange, data="Price Only" if args.simple or args.historic else "Full"
    )))

    influx = InfluxDB(args.database, args.host, args.port)

    if args.historic:
        data = Requester.history_minute(args.from_symbols, args.to_symbols, exchange=args.exchange)
        for (fromsym, tosym), values in data:
            datapoints = []
            for value in values:
                pricedata = {"FROMSYMBOL": fromsym, "TOSYMBOL": tosym, "PRICE": value["close"], "MARKET": args.exchange}
                datapoints.append(influx.make_price_pair(pricedata, value["time"]))
            influx.post_data(datapoints)
    else:
        cycle = Limiter(args.interval, 0)
        while True:
            try:
                if args.simple:
                    results = Requester.multi_price(args.from_symbols, args.to_symbols, args.exchange)
                else:
                    results = Requester.multi_price_full(args.from_symbols, args.to_symbols, args.exchange)
            except (requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                results = {}
                logger.warning("Failed Fetching Data")
            except ValueError as error:
                results = {}
                logger.warning("Bad Request: {}".format(error))
            if results:
                datapoints = []
                for fromsym, topoints in results.items():
                    for tosym, pricedata in topoints.items():
                        if fromsym == tosym:
                            continue
                        if args.simple:
                            pricedata = {
                                "FROMSYMBOL": fromsym, "TOSYMBOL": tosym, "PRICE": pricedata, "MARKET": args.exchange
                            }
                        datapoints.append(influx.make_price_pair(pricedata))

                influx.post_data(datapoints)
            if args.single:
                break
            cycle.check()
