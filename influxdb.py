import requests


class InfluxDB:

    def __init__(self, database, host="localhost", port=8086):
        self.database = database
        self.host = host
        self.port = port
        self.target = "http://{}:{}/{{}}".format(self.host, self.port)
        results = requests.post(self.target.format("query"),
                                params={"q": "CREATE DATABASE {}".format(database)},
                                )

    @classmethod
    def make_price_pair(cls, fromsym, tosym, fields=None):
        if fields is None:
            fields = {}
        fieldstring = ",".join(map(cls._format_fields, fields.items()))
        return "price,from={},to={} {}".format(fromsym, tosym, fieldstring)

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
