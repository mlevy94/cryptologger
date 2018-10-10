from time import sleep
from requester import Requester
from influxdb import InfluxDB


influx = InfluxDB("crypto", "192.168.10.50")
while True:
    results = Requester.multi_price_full(("BTC", "BCH", "XMR"))

    datapoints = []
    for fromsym, topoints in results.items():
        for tosym, pricedata in topoints.items():
            datapoints.append(influx.make_price_pair(fromsym, tosym, pricedata))

    ret = influx.post_data("\n".join(datapoints))
    print("Posting Price: {}".format(ret.status_code))
    sleep(10)
