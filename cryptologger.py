#!/usr/bin/env python
from argparse import ArgumentParser
from limiter import Limiter
from cryptocompare import CryptoCompare
from influxdb import InfluxDB
from functools import partial
import logging
import textwrap
import asyncio


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
        -Data: {data}""".format(
            host=args.host, port=args.port, database=args.database,
            interval="Single" if args.single else "Historic" if args.historic else "{} Seconds".format(args.interval),
            fromsyms=args.from_symbols, tosyms=args.to_symbols,
            exchange=args.exchanges, data="Price Only" if args.simple or args.historic else "Full"
    )))

    crypto = CryptoCompare()
    influx = InfluxDB(args.database, args.host, args.port)
    loop = asyncio.get_running_loop()

    if args.historic:
        futures = await crypto.history_minute(args.from_symbols, args.to_symbols, exchanges=args.exchanges)
        for future in asyncio.as_completed(futures):
            fromsym, tosym, exchange, values = await future
            datapoints = []
            for value in values:
                pricedata = {"FROMSYMBOL": fromsym, "TOSYMBOL": tosym, "PRICE": value["close"], "MARKET": exchange}
                datapoints.append(influx.make_price_pair(pricedata, value["time"]))
            asyncio.ensure_future(influx.post_data(datapoints), loop=loop)
    else:
        exchanges = {}
        to_symbols = set(args.to_symbols)
        for exchange in args.exchanges:
            pairings = {}
            if exchange in ["None", "CCCAGG", None]:
                exchange = None
                exchange_pairs = {}
            else:
                try:
                    exchange_pairs = crypto.exchanges[exchange]
                except KeyError:
                    continue
            for sym in args.from_symbols:
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
        if args.simple:
            func = crypto.multi_price
        else:
            func = crypto.multi_price_full
        for exchange, pairs in exchanges.items():
            for to_symbols, from_symbols in pairs.items():
                messages.append(
                    {"from_currencies": from_symbols, "to_currencies":to_symbols, "exchange": exchange}
                )
        cycle = Limiter(args.interval, 0)
        while True:
            datapoints = []
            coros = list(map(lambda kargs: func(**kargs), messages))
            for future in asyncio.as_completed(coros):
                try:
                    results, exchange = await future
                except ValueError as error:
                    logger.warning("Bad Request: {}".format(error))
                    continue
                if results:
                    for fromsym, topoints in results.items():
                        for tosym, pricedata in topoints.items():
                            if fromsym == tosym:
                                continue
                            if args.simple:
                                pricedata = {
                                    "FROMSYMBOL": fromsym, "TOSYMBOL": tosym,
                                    "PRICE": pricedata, "MARKET": exchange
                                }
                            datapoints.append(influx.make_price_pair(pricedata))

            await influx.post_data(datapoints)
            if args.single:
                break
            await cycle.check()

    tasks = asyncio.all_tasks()
    tasks.remove(asyncio.current_task())
    await asyncio.gather(*tasks)

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
    parser.add_argument("-e", "--exchanges", type=str, nargs="*", default=[None], help="Exchanges to get values from")
    parser.add_argument("-m", "--simple", action="store_true", help="Get just the price of each pairing")
    parser.add_argument("-y", "--historic", action="store_true", help="Get last 7 days of data")

    asyncio.run(main(parser.parse_args()))
