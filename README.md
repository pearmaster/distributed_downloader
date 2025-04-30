# distributed_downloader

```py
from distributed_downloader import DistributedDataset, UrlFetcher

from datetime import timedelta
from paho.mqtt.client import Client as MqttClient
from paho.mqtt.enums import CallbackAPIVersion

mqtt_client = MqttClient(callback_api_version=CallbackAPIVersion.VERSION2)
mqtt_client.connect_async("localhost")
mqtt_client.loop_start()

topic = "weather/utah/alerts"
url_fetcher = UrlFetcher("https://api.weather.gov/alerts/active.atom?area=UT", topic)

def print_alerts(topic, payload):
    print(payload)

distrib_data = DistributedDataset(mqtt_client, topic, min_age=timedelta(seconds=10), max_agetimedelta(seconds=300), fetcher=url_fetcher)
distrib_data.add_new_data_callback(print_alerts)
distrib_data.start_up(delay_seconds=3)

'''