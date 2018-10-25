#!/usr/bin/env python
from argparse import ArgumentParser
from limiter import Limiter
from cryptocompare import CryptoCompare
from influxdb import InfluxDB
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
        -Exchange: {exchange}
        -Data: {data}""".format(
            host=args.host, port=args.port, database=args.database,
            interval="Single" if args.single else "Historic" if args.historic else "{} Seconds".format(args.interval),
            fromsyms=args.from_symbols, tosyms=args.to_symbols,
            exchange=args.exchange, data="Price Only" if args.simple or args.historic else "Full"
    )))

    crypto = CryptoCompare()
    influx = InfluxDB(args.database, args.host, args.port)

    if args.historic:
        loop = asyncio.get_running_loop()

        async def process_data(fromsym, tosym, values):
            datapoints = []
            for value in values:
                pricedata = {"FROMSYMBOL": fromsym, "TOSYMBOL": tosym, "PRICE": value["close"], "MARKET": args.exchange}
                datapoints.append(influx.make_price_pair(pricedata, value["time"]))
            await asyncio.ensure_future(influx.post_data(datapoints), loop=loop)

        await crypto.history_minute(process_data, args.from_symbols, args.to_symbols, exchange=args.exchange)

    else:
        cycle = Limiter(args.interval, 0)
        while True:
            try:
                if args.simple:
                    results = await crypto.multi_price(args.from_symbols, args.to_symbols, args.exchange)
                else:
                    results = await crypto.multi_price_full(args.from_symbols, args.to_symbols, args.exchange)
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

                await influx.post_data(datapoints)
            if args.single:
                break
            await cycle.check()

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

    asyncio.run(main(args))
