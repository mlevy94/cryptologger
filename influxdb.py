import asyncio
import requests
import logging
import time


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
            point_time = time.time_ns()
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

    async def post_data(self, data):
        loop = asyncio.get_event_loop()
        futures = []
        for i in range(0, len(data), self.MAX_POST):
            await loop.run_in_executor(None, self._post_data, data[i:i + self.MAX_POST])
        await asyncio.gather(*futures)

    def _post_data(self, data):
        self.logger.info("Sending {} Data Points".format(len(data)))
        try:
            status = requests.post(
                self.target.format("write"), data="\n".join(data), headers={'Content-Type': 'application/octet-stream'},
                params={"db": self.database}
            )
            status.raise_for_status()
        except (requests.exceptions.ConnectionError,):
            self.logger.warning("Failed Sending Data")
        except requests.exceptions.RequestException:
            try:
                text = status.text
            except (AttributeError, NameError):
                text = ""
            self.logger.warning("Failed Sending Data: {}".format(text))
