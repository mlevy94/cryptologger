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
