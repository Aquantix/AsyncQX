from pika.spec import BasicProperties
from asyncqx.subscriber.subscriber import EventBinding
import pika.exceptions
import pytest

from asyncqx.subscriber import AQXSubscriber
from unittest import mock

import json

@pytest.fixture
def subscriber(rabbitmq):
    sub = AQXSubscriber(default_exchange='test_exchange')
    yield sub
    sub.close()


def test_bind_decorator(subscriber):
    
    mock_fn = mock.MagicMock()

    @subscriber.bind('some.event')
    def fn(event, payload, properties):
        pass
    
    assert subscriber._late_bindings[('test_exchange', '')][0].events == ('some.event',)
    assert subscriber._late_bindings[('test_exchange', '')][0].exclusive is False
    assert subscriber._late_bindings[('test_exchange', '')][0].callback.__wrapped__ == fn


def test_wrapping_function(subscriber):
    mock_fn = mock.MagicMock()

    subscriber.bind('some.event')(mock_fn)

    bound_fn = subscriber._late_bindings[('test_exchange', '')][0].callback
    props = BasicProperties(type='some.event')
    bound_fn(
        None, None, props, json.dumps({ 'hello': 'world' }))
    
    mock_fn.assert_called_once_with('some.event', { 'hello': 'world' }, props)

def test_wrapping_function_isolates_errors(subscriber):
    mock_fn = mock.MagicMock()
    mock_fn.side_effect = Exception('some exception')

    subscriber.bind('some.event')(mock_fn)

    bound_fn = subscriber._late_bindings[('test_exchange', '')][0].callback
    props = BasicProperties(type='some.event')

    try:
        bound_fn(
            None, None, props, json.dumps({'hello': 'world'}))
    except Exception:
        pytest.fail("Side effect was propagated")
