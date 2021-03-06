"""Combines the publisher and subscriber classes into a single interface"""

from asyncqx.core.types import Serializer, Stringable
from asyncqx.publisher.publisher import AQXPublisher
from asyncqx.subscriber.subscriber import AQXSubscriber


class AQXPubSub:
    """Provides the publishe and subscriber interfaces on a single object"""
    def __init__(self,
                 name,
                 amqp_url: str = None,
                 *,
                 default_exchange: Stringable = None,
                 default_serializer: Serializer = None,
                 default_queue: Stringable = None,
                 default_exclusive = False):
        self.publisher = AQXPublisher(
            name, amqp_url, default_exchange=default_exchange,
            default_serializer=default_serializer)

        self.subscriber = AQXSubscriber(
            amqp_url,
            default_exchange=default_exchange,
            default_serializer=default_serializer,
            default_queue=default_queue,
            default_exclusive=default_exclusive)

    def emit(self,
             event: Stringable,
             payload: object,
             *,
             mandatory=False,
             correlation_id=None,
             headers: object = None,
             exchange: Stringable = None,
             serializer: Serializer = None) -> None:
        return self.publisher.emit(
            event,
            payload,
            mandatory=mandatory,
            correlation_id=correlation_id,
            headers=headers,
            exchange=exchange,
            serializer=serializer)

    def bind(self,
             *events: Stringable,
             queue_name: Stringable = None,
             exchange: Stringable = None,
             exclusive: bool = False,
             serializer: Serializer = None):
        return self.subscriber.bind(
            *events,
            queue_name=queue_name,
            exchange=exchange,
            exclusive=exclusive,
            serializer=serializer)

    def consume(self):
        return self.subscriber.consume()
