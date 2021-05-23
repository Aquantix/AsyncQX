from typing import Dict, Protocol, runtime_checkable

from asyncqx.core.types import EventListener, Serializer, Stringable


@runtime_checkable
class Subscriber(Protocol):

    def require(self,
                roles: tuple = None,
                claims: tuple = None,
                func: EventListener = None): ...

    def bind(self,
             *events: Stringable,
             queue_name: Stringable = '',
             exchange=None,
             serializer: Serializer = None): ...

    def consume(self,
                exclusive: bool = False): ...
