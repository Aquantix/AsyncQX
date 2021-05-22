import json
import logging
from typing import Type

import pika
import pika.channel
import pika.exceptions
from pika.adapters import blocking_connection
from retry import retry

LOGGER = logging.getLogger(__name__)


class AQXBase:
    RETRY_DELAY = 5  # seconds
    RETRY_JITTER = (1, 3)  # seconds

    def __init__(self, amqp_url: str = None):
        self._url = amqp_url

        self._connection = None
        self._channel = None

    @property
    def connection(self) -> blocking_connection.BlockingConnection:
        self._ensure_connection()
        return self._connection  # type: ignore

    @property
    def channel(self) -> blocking_connection.BlockingChannel:
        self._ensure_channel()
        return self._channel  # type: ignore

    @retry(pika.exceptions.AMQPConnectionError, delay=RETRY_DELAY, jitter=RETRY_JITTER, logger=LOGGER)
    def connect(self):
        self._prepare_to_connect()
        self._connect()

    def _connect(self):
        LOGGER.info('creating blocking connection')
        connection_parameters = pika.URLParameters(
            self._url) if self._url else None
        self._connection = pika.BlockingConnection(connection_parameters)

        self._open_channel()

    def close(self):
        try:
            if self._connection:
                LOGGER.info('closing connection')
                self._connection.close()
        except:
            pass

    def _prepare_to_connect(self):
        if self._connection and self._connection.is_open:
            LOGGER.info('closing connection')
            self._connection.close()

        if self._channel and self._channel.is_open:
            try:
                LOGGER.info('closing channel')
                self._channel.close()
            except:
                pass

    def _open_channel(self):
        LOGGER.info('opening channel')
        self._channel = self._connection.channel()
        LOGGER.info('setting channel to confirm delivery')
        self._channel.confirm_delivery()

    def _ensure_connection(self):
        LOGGER.debug('ensuring connection is ready')
        if self._connection is None:
            self.connect()

        if self._connection.is_closed:
            try:
                self._connection.close()
            finally:
                self.connect()

    def _ensure_channel(self):
        LOGGER.debug('ensuring channel is ready')
        self._ensure_connection()

        if self._channel is None or self._channel.is_closed:
            self._open_channel()


class JSONSerializer:

    def __init__(self, cls: Type[json.JSONEncoder] = None) -> None:
        if cls is None:
            LOGGER.info('using default JSONEncoder')
        self.encoder = cls or json.JSONEncoder

    def decode(self, data: bytes) -> object:
        return json.loads(data)

    def encode(self, data: object) -> bytes:
        return json.dumps(data, cls=self.encoder).encode()
