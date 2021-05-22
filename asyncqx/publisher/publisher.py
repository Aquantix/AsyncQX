import json
import logging
import time

import pika
import pika.exceptions
from asyncqx.core.base import AQXBase, JSONSerializer
from asyncqx.core.types import Serializer, Stringable
from retry import retry

LOGGER = logging.getLogger(__name__)


class AQXPublisher (AQXBase):
    MAX_TRIES: int = 5
    RETRY_DELAY: int = 0

    def __init__(self,
                 name: str,
                 amqp_url: str = None,
                 *,
                 default_exchange=None,
                 default_serializer: Serializer = None):
        super().__init__(amqp_url)

        self.name = str(name)

        self.default_exchange = default_exchange or 'asyncqx'
        self.default_serializer = default_serializer or JSONSerializer()

    def emit(self,
             event: Stringable,
             payload: object,
             *,
             correlation_id=None,
             exchange: Stringable = None,
             serializer: Serializer = None) -> None:

        exchange = exchange or self.default_exchange
        serializer = serializer or self.default_serializer

        LOGGER.info('emitting event: event=%s exchange=%s', event, exchange)

        props = pika.BasicProperties(
            app_id=self.name,
            type=str(event),
            timestamp=int(time.time()),
            headers=
            correlation_id=str(correlation_id) if correlation_id else None)

        data = serializer.encode(payload)

        self._publish(str(exchange),
                      routing_key=str(event),
                      properties=props,
                      data=data)

    @retry(pika.exceptions.AMQPConnectionError, tries=MAX_TRIES, delay=RETRY_DELAY, logger=LOGGER)
    def _publish(self, exchange: str, routing_key: str, properties: pika.BasicProperties, data: bytes):
        self._ensure_channel()

        self.channel.exchange_declare(exchange,
                                      exchange_type='topic',
                                      durable=True)

        try:
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=properties,
                body=data)

        except pika.exceptions.UnroutableError as err:
            LOGGER.debug('message was unroutable: %s', err)
            pass
