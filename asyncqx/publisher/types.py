from typing import Dict, Protocol, runtime_checkable

from asyncqx.core.types import Serializer, Stringable


@runtime_checkable
class Publisher(Protocol):

    def emit(self,
             event: Stringable,
             payload: object,
             *,
             correlation_id=None,
             headers: Dict = None,
             exchange: Stringable = None,
             serializer: Serializer = None) -> None: ...
