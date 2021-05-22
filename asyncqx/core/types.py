from typing import Protocol

from pika.spec import BasicProperties


class Stringable(Protocol):

    def __str__(self) -> str: ...


class Serializer(Protocol):

    def decode(self, data: bytes) -> object: ...

    def encode(self, data: object) -> bytes: ...


class EventListener(Protocol):

    def __call__(self, event: str, payload: object,
                 props: BasicProperties): ...
