import logging
import time

import pika
import pika.exceptions
from retry import retry

from asyncqx.core.base import AQXBase, JSONSerializer
from asyncqx.core.types import Serializer, Stringable

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
             mandatory=False,
             correlation_id=None,
             headers: object = None,
             exchange: Stringable = None,
             serializer: Serializer = None) -> None:

        exchange = exchange or self.default_exchange
        serializer = serializer or self.default_serializer

        LOGGER.info('emitting event: event=%s exchange=%s', event, exchange)

        props = pika.BasicProperties(
            app_id=self.name,
            type=str(event),
            timestamp=int(time.time()),
            headers=headers,
            delivery_mode=2 if mandatory else 1,
            correlation_id=str(correlation_id) if correlation_id else None)

        data = serializer.encode(payload)

        self._publish(str(exchange),
                      routing_key=str(event),
                      properties=props,
                      mandatory=mandatory,
                      data=data)

    @retry(pika.exceptions.AMQPConnectionError, tries=MAX_TRIES, delay=RETRY_DELAY, logger=LOGGER)
    def _publish(self, exchange: str, routing_key: str,
                 properties: pika.BasicProperties,
                 mandatory: bool, data: bytes):
        self._ensure_channel()

        try:
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=properties,
                mandatory=mandatory,
                body=data)

        except pika.exceptions.UnroutableError as err:
            LOGGER.debug('message %s to exchange %s was unroutable: %s',
                         routing_key, exchange, err)
            if mandatory:
                raise

        except pika.exceptions.ChannelClosedByBroker as err:
            if err.reply_code == 404:  # exchange not found
                self.channel.exchange_declare(exchange,
                                              exchange_type='topic',
                                              durable=True)
                # Retry now that the exchange is declared
                self._publish(exchange, routing_key,
                              properties, mandatory, data)
            else:
                raise
