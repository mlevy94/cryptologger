from time import sleep
from argparse import ArgumentParser
import requests


class Requester:

    CC_API = "https://min-api.cryptocompare.com/data/{}"
    APP_NAME = "InfluxDB Logger"

    @classmethod
    def multi_price(cls, from_currencies=("BTC",), to_currencies=("USD",), exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": cls.APP_NAME,
        }
        result = requests.get(cls.CC_API.format("pricemulti"), params=payload)
        return result.json()

    @classmethod
    def multi_price_full(cls, from_currencies=("BTC",), to_currencies=("USD",), exchange=None):
        payload = {
            "fsyms": ",".join(from_currencies),
            "tsyms": ",".join(to_currencies),
            "e": exchange,
            "extraParams": cls.APP_NAME,
        }
        result = requests.get(cls.CC_API.format("pricemultifull"), params=payload)
        return result.json().get("RAW", {})


class InfluxDB:

    def __init__(self, database, host="localhost", port=8086):
        self.database = database
        self.host = host
        self.port = port
        self.target = "http://{}:{}/{{}}".format(self.host, self.port)
        try:
            results = requests.post(self.target.format("query"),
                                    params={"q": "CREATE DATABASE {}".format(database)},
                                    )
        except requests.exceptions.ConnectionError:
            raise ConnectionError("Failed connecting to InfluxDB at {}".format(self.target.format("")))
        results.raise_for_status()

    @classmethod
    def make_price_pair(cls, fromsym, tosym, fields=None):
        if fields is None:
            fields = {}
        market = fields.pop("MARKET", None)
        # remove unnecessary fields
        fields.pop("FROMSYMBOL", None)
        fields.pop("TOSYMBOL", None)
        tags = {"from": fromsym, "to": tosym}
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
        return requests.post(self.target.format("write"), data=data,
                             headers={'Content-Type': 'application/octet-stream'}, params={"db": self.database})


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-s", "--single", action="store_true", help="Single run only")
    parser.add_argument("-i", "--interval", type=float, default=10.0,
                        help="Interval between queries in seconds (default: 10)")
    parser.add_argument("-o", "--host", default="localhost", help="InfluxDB host address (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=8086, help="InfluxDB port (default: 8086)")
    parser.add_argument("-d", "--database", default="crypto", help="InfluxDB database (default: crypto)")
    parser.add_argument("-f", "--from-symbols", type=str, nargs="*", default=("BTC", "BCH", "ETH"),
                        help="List of from symbols (default: BTC BCH ETH)")
    parser.add_argument("-t", "--to-symbols", type=str, nargs="*", default=("USD",),
                        help="List of to symbols (default: USD)")
    parser.add_argument("-e", "--exchange", type=str, default=None, help="Exchange to get values from")
    parser.add_argument("-m", "--simple", action="store_true", help="Get just the price of each pairing")
    args = parser.parse_args()

    influx = InfluxDB(args.database, args.host, args.port)

    while True:
        if args.simple:
            results = Requester.multi_price(args.from_symbols, args.to_symbols, args.exchange)
        else:
            results = Requester.multi_price_full(args.from_symbols, args.to_symbols, args.exchange)

        datapoints = []
        for fromsym, topoints in results.items():
            for tosym, pricedata in topoints.items():
                if args.simple:
                    pricedata = {"PRICE": pricedata, "MARKET": args.exchange}
                datapoints.append(influx.make_price_pair(fromsym, tosym, pricedata))

        print(datapoints)
        # ret = influx.post_data("\n".join(datapoints))
        # print("Posting Code: {}".format(ret.status_code))
        if args.single:
            break
        sleep(args.interval)
