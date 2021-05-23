import pika
from asyncqx.subscriber.subscriber import EventBinding, create_event_switch

from unittest import mock


def test_with_a_single_binding_the_one_callback_is_called():
    mock_callback = mock.MagicMock()

    event_bindings = [
        EventBinding(
            events=('some.event',),
            callback=mock_callback,
            exclusive=False
        )]

    switch = create_event_switch(event_bindings)

    mock_properties = mock.MagicMock(spec=pika.BasicProperties)
    mock_properties.type = 'some.event'
    switch(None, None, mock_properties, None)

    mock_callback.assert_called_once()


def test_with_multiple_bindings():
    mock_callback_a = mock.MagicMock()
    mock_callback_b = mock.MagicMock()

    event_bindings = [
        EventBinding(
            events=('some.event.*',),
            callback=mock_callback_a,
            exclusive=False
        ),
        EventBinding(
            events=('some.event.*',),
            callback=mock_callback_b,
            exclusive=False
        )
    ]

    switch = create_event_switch(event_bindings)
    
    mock_properties = mock.MagicMock(spec=pika.BasicProperties)
    mock_properties.type = 'some.event.type'
    switch(None, None, mock_properties, None)

    mock_callback_a.assert_called_once()
    mock_callback_b.assert_called_once()
