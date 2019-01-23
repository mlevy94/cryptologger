#!/usr/bin/env python
from argparse import ArgumentParser
from limiter import Limiter
from cryptocompare import CryptoCompare
from influxdb import InfluxDB
import logging
import textwrap
import asyncio


class CryptoLogger:

    def __init__(self, database, api_key, host="localhost", port=8086, *args, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.crypto = CryptoCompare(api_key)
        self.influx = InfluxDB(database=database, host=host, port=port)
        self.loop = asyncio.get_event_loop()

    async def get_minute_history(self, from_symbols, to_symbols, exchanges):
        futures = await self.crypto.history_minute(from_symbols, to_symbols, exchanges=exchanges)
        for future in asyncio.as_completed(futures):
            fromsym, tosym, exchange, values = await future
            datapoints = []
            for value in values:
                pricedata = {"FROMSYMBOL": fromsym, "TOSYMBOL": tosym, "PRICE": value["close"], "MARKET": exchange}
                datapoints.append(self.influx.make_price_pair(pricedata, value["time"]))
            asyncio.ensure_future(self.influx.post_data(datapoints), loop=self.loop)

    async def make_messages(self, from_symbols, to_symbols, exchanges_list):
        exchanges = {}
        to_symbols = set(to_symbols)
        for exchange in exchanges_list:
            pairings = {}
            if exchange in ["None", "CCCAGG", None]:
                exchange = None
                exchange_pairs = {}
            else:
                try:
                    exchange_pairs = self.crypto.exchanges[exchange]
                except KeyError:
                    continue
            for sym in from_symbols:
                if exchange is None:
                    # avoid self matches
                    match = to_symbols.difference([sym], [])
                else:
                    match = exchange_pairs.get(sym, [])
                sym_pair = tuple(to_symbols.intersection(match))
                if sym_pair:
                    try:
                        pairings[sym_pair].append(sym)
                    except KeyError:
                        pairings[sym_pair] = [sym]
            if pairings:
                exchanges[exchange] = pairings

        # flatten
        messages = []
        for exchange, pairs in exchanges.items():
            for to_syms, from_syms in pairs.items():
                messages.append(
                    {"from_currencies": from_syms, "to_currencies": to_syms, "exchange": exchange}
                )
        return messages

    async def get_current_values(self, messages, interval, simple=False, single=False):
        if simple:
            func = self.crypto.multi_price
        else:
            func = self.crypto.multi_price_full
        cycle = Limiter(interval, 0)
        while True:
            datapoints = []
            coros = list(map(lambda kargs: func(**kargs), messages))
            for future in asyncio.as_completed(coros):
                try:
                    results, exchange = await future
                except ValueError as error:
                    self.logger.warning("Bad Request: {}".format(error))
                    continue
                if results:
                    for fromsym, topoints in results.items():
                        for tosym, pricedata in topoints.items():
                            if fromsym == tosym:
                                continue
                            if simple:
                                pricedata = {
                                    "FROMSYMBOL": fromsym, "TOSYMBOL": tosym,
                                    "PRICE": pricedata, "MARKET": exchange
                                }
                            datapoints.append(self.influx.make_price_pair(pricedata))

            await self.influx.post_data(datapoints)
            if single:
                break
            await cycle.check()


async def main(args):
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
        -Exchanges: {exchange}
        -Historic: {historic}
        -Data: {data}""".format(
            host=args.host, port=args.port, database=args.database, historic=args.historic,
            interval="Single" if args.single else "{} Seconds".format(args.interval),
            fromsyms=args.from_symbols, tosyms=args.to_symbols,
            exchange=args.exchanges, data="Price Only" if args.simple else "Full"
    )))

    cryptologger = CryptoLogger(args.database, args.key, args.host, args.port)

    if args.historic:
        asyncio.ensure_future(cryptologger.get_minute_history(args.from_symbols, args.to_symbols, args.exchanges))
    messages = await cryptologger.make_messages(args.from_symbols, args.to_symbols, args.exchanges)
    await cryptologger.get_current_values(messages, args.interval, args.simple, args.single)

    tasks = asyncio.all_tasks()
    tasks.remove(asyncio.current_task())
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("key", help="CryptoCompare API Key")
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
    parser.add_argument("-e", "--exchanges", type=str, nargs="*", default=[None], help="Exchanges to get values from")
    parser.add_argument("-m", "--simple", action="store_true", help="Get just the price of each pairing")
    parser.add_argument("-y", "--historic", action="store_true", help="Get last 7 days of data before current data")

    asyncio.run(main(parser.parse_args()))
