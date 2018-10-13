from time import sleep
from argparse import ArgumentParser
import requests
import logging
import textwrap


class Requester:

    CC_API = "https://min-api.cryptocompare.com/data/{}"
    APP_NAME = "InfluxDB Logger"
    logger = logging.getLogger("Requester")

    @classmethod
    def multi_price(cls, from_currencies, to_currencies, exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": cls.APP_NAME,
        }
        cls.logger.info("Simple: {} -> {}".format(from_currencies, to_currencies))
        result = requests.get(cls.CC_API.format("pricemulti"), params=payload)
        return result.json()

    @classmethod
    def multi_price_full(cls, from_currencies, to_currencies, exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": cls.APP_NAME,
        }
        cls.logger.info("Full: {} -> {}".format(from_currencies, to_currencies))
        result = requests.get(cls.CC_API.format("pricemultifull"), params=payload)
        return result.json().get("RAW", {})


class InfluxDB:

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
    def make_price_pair(cls, fields):
        tags = {"from": fields.pop("FROMSYMBOL"), "to": fields.pop("TOSYMBOL")}
        market = fields.pop("MARKET", None)
        if market is not None:
            tags["market"] = market
        tagstring = ",".join("{}={}".format(key, val) for key, val in tags.items())
        fieldstring = ",".join(map(cls._format_fields, fields.items()))
        return "price,{} {}".format(tagstring, fieldstring)

    @staticmethod
    def _format_fields(pair):
        key, val = pair
        if isinstance(val, str):
            val = '"{}"'.format(val)
        elif isinstance(val, bool):
            val = str(val)
        return "{}={}".format(key, val)

    def post_data(self, data):
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
            interval="Single" if args.single else "{} Seconds".format(args.interval),
            fromsyms=args.from_symbols, tosyms=args.to_symbols,
            exchange=args.exchange, data="Price Only" if args.simple else "Full"
    )))

    influx = InfluxDB(args.database, args.host, args.port)

    while True:
        if args.simple:
            results = Requester.multi_price(args.from_symbols, args.to_symbols, args.exchange)
        else:
            results = Requester.multi_price_full(args.from_symbols, args.to_symbols, args.exchange)

        datapoints = []
        for fromsym, topoints in results.items():
            for tosym, pricedata in topoints.items():
                if fromsym == tosym:
                    continue
                if args.simple:
                    pricedata = {"FROMSYMBOL": fromsym, "TOSYMBOL": tosym, "PRICE": pricedata, "MARKET": args.exchange}
                datapoints.append(influx.make_price_pair(pricedata))

        influx.post_data(datapoints)
        if args.single:
            break
        sleep(args.interval)
