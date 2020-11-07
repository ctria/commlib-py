import functools
import sys
import time
import json
import uuid
import datetime

from collections import deque
from threading import Semaphore, Thread, Event as ThreadEvent
import logging
from typing import OrderedDict, Any
from inspect import signature
from enum import IntEnum

from commlib.logger import Logger, LoggingLevel
from commlib.serializer import ContentType
from commlib.rpc import BaseRPCService, BaseRPCClient
from commlib.pubsub import BasePublisher, BaseSubscriber
from commlib.action import (
    BaseActionServer, BaseActionClient, _ActionGoalMessage,
    _ActionResultMessage, _ActionGoalMessage, _ActionCancelMessage,
    _ActionStatusMessage, _ActionFeedbackMessage
)
from commlib.events import BaseEventEmitter, Event
from commlib.msg import RPCMessage, PubSubMessage, ActionMessage
from commlib.utils import gen_timestamp

import paho.mqtt.client as mqtt


class MQTTReturnCode(IntEnum):
    CONNECTION_SUCCESS = 0
    INCORRECT_PROTOCOL_VERSION = 1
    INVALID_CLIENT_ID = 2
    SERVER_UNAVAILABLE = 3
    AUTHENTICATION_ERROR = 4
    AUTHORIZATION_ERROR = 5


class MQTTProtocolType(IntEnum):
    MQTTv31 = 1
    MQTTv311 = 2


class Credentials(object):
    def __init__(self, username='', password=''):
        self.username = username
        self.password = password


class ConnectionParameters(object):
    __slots__ = ['host', 'port', 'creds', 'protocol']
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 1883,
                 protocol: MQTTProtocolType = MQTTProtocolType.MQTTv311,
                 creds: Credentials = Credentials()):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.creds = creds


class MQTTTransport(object):
    def __init__(self, conn_params=ConnectionParameters(), logger=None):
        self._conn_params = conn_params
        self._logger = logger
        self._connected = False
        self._data_clb = None

        self.logger = Logger(self.__class__.__name__) if \
            logger is None else logger

        self._client = mqtt.Client(clean_session=True,
                                   protocol=mqtt.MQTTv311,
                                   transport='tcp')

        self._client.on_connect = self.on_connect
        self._client.on_disconnect = self.on_disconnect
        # self._client.on_log = self.on_log
        self._client.on_message = self.on_message

        self._client.username_pw_set(self._conn_params.creds.username,
                                     self._conn_params.creds.password)

        self._client.connect(self._conn_params.host, self._conn_params.port, 60)

    def on_connect(self, client, userdata, flags, rc):
        if rc == MQTTReturnCode.CONNECTION_SUCCESS:
            self.logger.debug(
                f"Connected to MQTT broker <{self._conn_params.host}:{self._conn_params.port}>")
            self._connected = True

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            self.logger.warn("Unexpected disconnection from MQTT Broker.")

    def on_message(self, client, userdata, msg):
        # print(f"{msg.topic}:{msg.payload}")
        _topic = msg.topic
        _payload = json.loads(msg.payload)
        if self._data_clb is not None:
            self._data_clb(_topic, _payload)

    def on_log(self, client, userdata, level, buf):
        self.logger.debug(f'MQTT Log: {buf}')

    def publish(self, topic: str, payload: dict, qos: int = 0,
                retain: bool = False,
                confirm_delivery: bool = False):
        topic = topic.replace('.', '/')
        ph = self._client.publish(topic, payload, qos=qos, retain=retain)
        if confirm_delivery:
            ph.wait_for_publish()

    def subscribe(self, topic: str, callback: callable, qos: int = 0):
        # self._client.message_callback_add(topic, callback)
        self._data_clb = callback
        self._client.subscribe(topic, qos)

    def start_loop(self):
        self._client.loop_start()


class Publisher(BasePublisher):
    def __init__(self,
                 conn_params: ConnectionParameters = ConnectionParameters(),
                 *args, **kwargs):
        self._msg_seq = 0
        self.conn_params = conn_params
        super(Publisher, self).__init__(*args, **kwargs)
        self._transport = MQTTTransport(conn_params=conn_params,
                                        logger=self._logger)
        self._transport.start_loop()

    def publish(self, msg: PubSubMessage) -> None:
        if self._msg_type is None:
            data = msg
        else:
            data = msg.as_dict()
        _msg = self._prepare_msg(data)
        _msg = self._serializer.serialize(_msg)
        # self.logger.debug(
        #     f'Publishing Message: <{self._topic}>:{data}')
        self._transport.publish(self._topic, _msg)
        self._msg_seq += 1

    def _prepare_msg(self, data):
        meta = {
            'timestamp': int(datetime.datetime.now(
                datetime.timezone.utc).timestamp() * 1000000),
            'properties': {
                'content_type': self._serializer.CONTENT_TYPE,
                'content_encoding': self._serializer.CONTENT_ENCODING
            }
        }
        _msg = {
            'data': data,
            'meta': meta
        }
        return _msg


class Subscriber(BaseSubscriber):
    def __init__(self,
                 conn_params: ConnectionParameters = ConnectionParameters(),
                 *args, **kwargs):
        self.conn_params = conn_params
        super(Subscriber, self).__init__(*args, **kwargs)
        self._transport = MQTTTransport(conn_params=conn_params,
                                        logger=self._logger)

    def run(self):
        self._transport.subscribe(self._topic,
                                  self._on_message)
        self._transport.start_loop()
        self.logger.info(f'Started Subscriber: <{self._topic}>')

    def _on_message(self, topic: str, payload: dict):
        try:
            data = payload['data']
            meta = payload['meta']
            if self.onmessage is not None:
                if self._msg_type is None:
                    _clb = functools.partial(self.onmessage, OrderedDict(data))
                else:
                    _clb = functools.partial(self.onmessage, self._msg_type(**data))
                _clb()
        except Exception:
            self.logger.error('Exception caught in _on_message', exc_info=True)
