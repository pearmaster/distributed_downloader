
from typing import Optional, Callable, Tuple
from datetime import timedelta
from threading import Timer
import paho.mqtt.client as paho
import paho.mqtt.properties as paho_props
from paho.mqtt.packettypes import PacketTypes
import random
import logging
import requests
from functools import partial

Token = Optional[str]
LastModified = Optional[str]
Fetcher = Callable[[Token, LastModified], Tuple[list[Tuple[str, str]], LastModified]]
NewDataCallback = Callable[[str, str], None]

class DistributedDataset:

    def __init__(self, mqtt_client: paho.Client, topic: str, min_age: timedelta, max_age: timedelta, fetcher: Optional[Fetcher], token: Token=None):
        self._client = mqtt_client
        self._topic = topic
        self._fetcher = fetcher
        self._token = token or ""
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{self._token})
        self._client.add_subscription(self._topic, callback=self._mqtt_received_data)
        self._new_data_callbacks: list[NewDataCallback] = list()
        self._min_age = min_age
        self._max_age = max_age
        self._timer: Timer|None = None
        self._start_up_timer: Timer|None = None
        self.last_data = None
        self.last_modified = None

    def __del__(self):
        self._timer.cancel()

    def _reset_timer(self, delay_seconds: Optional[int]=None):
        if delay_seconds is None:
            delay_seconds = random.randint(int(self._min_age.total_seconds()), int(self._max_age.total_seconds()))
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._logger.debug("%s Triggering next data fetch in %d seconds", self._token, delay_seconds)
        self._timer = Timer(delay_seconds, self._timer_expired)
        self._timer.start()

    def _timer_expired(self):
        if self._start_up_timer is not None:
            self._start_up_timer.cancel()
            self._start_up_timer = None
        self._logger.info("%s Fetching data with %s", self._token, self._fetcher)
        self._reset_timer()
        if self._fetcher is not None:
            results, last_modified = self._fetcher(self._token, self.last_modified)
            if last_modified is not None:
                self.last_modified = last_modified
            for topic, payload in results:
                if self.last_modified is not None:
                    publish_props = paho_props.Properties(PacketTypes.PUBLISH)
                else:
                    publish_props = None
                self._client.publish(topic, payload, qos=1, retain=True, publish_props=publish_props)

    def fetch_now(self, delay_seconds: Optional[int]=None):
        if delay_seconds is None:
            self._timer_expired()
        else:
            self._reset_timer(delay_seconds)

    def _mqtt_received_data(self, msg: paho.MQTTMessage):
        self._logger.debug("%s got data from %s", self._token, msg.topic)
        if self._start_up_timer is not None:
            self._start_up_timer.cancel()
            self._start_up_timer = None
        self.last_data = msg.payload.decode()
        self._reset_timer()
        for cb in self._new_data_callbacks:
            cb(msg.topic, self.last_data)

    def add_new_data_callback(self, cb: NewDataCallback):
        self._new_data_callbacks.append(cb)

    def _start_up_timer_expired(self):
        self._logger.debug("%s Priming data with fetch", self._token)
        if self.last_data is None:
            self.fetch_now()

    def start_up(self, delay_seconds: Optional[float]=None):
        self._start_up_timer = Timer(delay_seconds, self._start_up_timer_expired)
        self._start_up_timer.start()


class UrlFetcher:

    def __init__(self, url: str, topic: str, http_headers: dict[str, str]):
        self.url = url
        self.default_topic = topic
        self.filter: Callable[[str], list[Tuple[str,str]]] = self.default_filter
        self.headers = headers

    def default_filter(self, payload: str) -> list[Tuple[str,str]]:
        return [(self.default_topic, payload)]

    def set_filter(self, filter: Callable[[str], list[Tuple[str,str]]]):
        self.filter = filter

    def __call__(self, token: str, last_modified: LastModified) -> Tuple[list[Tuple[str, str]], LastModified]:
        if last_modified is not None:
            headers['If-Modified-Since'] = last_modified
        resp = requests.get(self.url, headers=self.headers)
        new_last_modified = resp.headers.get("Last-Modified", resp.headers.get("Date"))
        return_value = self.filter(resp.text)
        return return_value, new_last_modified
