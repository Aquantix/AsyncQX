import functools
import logging
import traceback
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Tuple

import pika.exceptions
from asyncqx.core.base import AQXBase, JSONSerializer
from asyncqx.core.types import EventListener, Serializer, Stringable
from asyncqx.tools import amqp_match
from retry import retry

LOGGER = logging.getLogger(__name__)


@dataclass
class EventBinding:
    """A dataclass representing the binding of a callback
    to one or more events."""
    events: Tuple[Stringable, ...]
    callback: Callable
    exclusive: bool


class AQXSubscriber (AQXBase):
    RETRY_DELAY = AQXBase.RETRY_DELAY
    RETRY_JITTER = AQXBase.RETRY_JITTER

    def __init__(self,
                 amqp_url: str,
                 *,
                 default_serializer=None,
                 default_exchange=None,
                 default_queue=None):
        super().__init__(amqp_url=amqp_url)

        self.default_serializer = default_serializer or JSONSerializer()
        self.default_exchange = default_exchange or 'asyncqx'
        self.default_queue = default_queue or ''

        self._late_bindings: Dict[Tuple[Stringable,
                                        Stringable], List[EventBinding]] = {}

    def bind(self,
             *events: Stringable,
             queue_name: Stringable = None,
             exchange: Stringable = None,
             exclusive: bool = False,
             serializer: Serializer = None):
        """Create a decorate to bind callbacks to one or more events.

        Args:
            queue_name (Stringable, optional): The name of the queue to receive events from. If None then the default is used.
            exchange (Stringable, optional): The exchange the queue is bound to. If None then the default is used.
            exclusive (bool, optional): If the subscriber should be the only one. Defaults to False.
            serializer (Serializer, optional): The Serializer to use for deserializing messages. If None then the default is used.

        Returns:
            Decorator: Returns a decorator which can bind function as an event callback.
        """

        exchange = exchange or self.default_exchange
        serializer = serializer or self.default_serializer
        queue_name = queue_name or self.default_queue

        def decorator(callback: EventListener):
            """Binds an EventListener callback to the specified events and queue.

            Args:
                callback (EventListener): [description]

            Returns:
                EventListener: Returns the original callback
            """
            LOGGER.info(
                'late-binding callback %s to events %s from queue %s',
                callback, events, queue_name)

            @functools.wraps(callback)
            def message_callback(unused_ch, unused_method, props, body):
                event = props.type
                source = props.app_id

                LOGGER.info(
                    'received message type %s from source %s of length %s',
                    event, source, len(body))

                try:
                    data = serializer.decode(body)
                    callback(event, data, props)

                except Exception as error:
                    LOGGER.error(traceback.format_exc())
                    LOGGER.error(
                        'error in callback for message type %s from source %s: %s',
                        event, source, error)

            self._add_late_binding(
                exchange,
                queue_name,
                events,
                exclusive,
                callback=message_callback)

            return callback

        return decorator

    @retry(pika.exceptions.AMQPConnectionError, delay=RETRY_DELAY, jitter=RETRY_JITTER)
    def consume(self):
        """Create all queues and exchanges and begin consuming events.

        Delayed retries on AQMPConnectionErrors
        """
        self._ensure_channel()
        self._apply_late_bindings()

        try:
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            pass

    def _apply_late_bindings(self):
        """For each binding, create the queue and exchange and bind the callback.

        If more than one binding has been created for any combindation of queue - exchange
        then a switching function is created to act as an intermediary between the queue
        and the multiple bound functions.
        """
        for (exchange, queue), event_bindings in self._late_bindings.items():
            assert len(event_bindings) > 0, (
                'exchange-queue combo is present but event_bindings is empty', exchange, queue)

            exclusive = bool(queue == '') or any(
                eb.exclusive for eb in event_bindings)

            self._declare_queue_exchange(exchange, queue, exclusive)

            if len(event_bindings) == 1:
                message_callback = event_bindings[0].callback
            else:
                message_callback = create_event_switch(event_bindings)

            for event_binding in event_bindings:
                for event in event_binding.events:
                    self._channel.queue_bind(
                        exchange=exchange,
                        queue=queue,
                        routing_key=event)

            self._channel.basic_consume(
                queue=queue,
                on_message_callback=message_callback,
                auto_ack=True)

    def _declare_queue_exchange(self, exchange, queue, exclusive):
        self._channel.exchange_declare(
            exchange,
            exchange_type='topic',
            durable=True)

        self._channel.queue_declare(
            queue,
            durable=not exclusive,
            exclusive=exclusive,
            auto_delete=exclusive)

    def _add_late_binding(self, exchange, queue_name, events, exclusive, callback):
        binding_key = (exchange, queue_name)
        self._late_bindings.setdefault(binding_key, [])
        self._late_bindings[binding_key].append(
            EventBinding(
                events=events,
                callback=callback,
                exclusive=exclusive))


def create_event_switch(event_bindings: Iterable[EventBinding]) -> Callable:
    """Returns a function that will switch callbacks based on the event type.

    This allows multiple different callbacks to be bound to the same queue for
    cleaner code.

    Args:
        event_bindings (Iterable[EventBinding]): [description]

    Returns:
        Callable: [description]
    """
    def switch(ch, method, props, body):
        event = props.type

        for event_binding in event_bindings:
            if any(amqp_match(event, str(pattern)) for pattern in event_binding.events):
                try:
                    event_binding.callback(ch, method, props, body)
                except Exception as err:
                    LOGGER.error(traceback.format_exc())
                    LOGGER.error('error switching event %s: %s', event, err)

    return switch
