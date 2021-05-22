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


def test_get_jwt(publisher):
    jwt = { 'sub': 'test' }
    publisher.jwt = jwt

    assert publisher.get_jwt() == jwt


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
