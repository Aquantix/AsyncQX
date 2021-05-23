import pika.exceptions
import pytest

from asyncqx.publisher import AQXPublisher


@pytest.fixture
def publisher(rabbitmq):
    pub = AQXPublisher('test',
                       default_exchange='test_exchange')
    yield pub
    pub.close()


def test_publisher_can_connect(publisher):
    publisher.connect()

    assert publisher.connection.is_open


def test_pub_can_emit_message(publisher: AQXPublisher):
    publisher.emit(
        'event.test',
        {
            'hello': 'world'
        })


def test_pub_reestablishes_connection(publisher: AQXPublisher):
    publisher.close()
    publisher.emit(
        'event.test',
        {
            'hello': 'world'
        })


def test_mandatory_emit_fails_when_no_consumer(publisher: AQXPublisher):
    with pytest.raises(pika.exceptions.UnroutableError):
        publisher.emit(
            'event.test',
            {
                'hello': 'world'
            },
            mandatory=True
        )
