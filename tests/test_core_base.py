from asyncqx.core import AQXBase


def test_aqxbase_can_connect(rabbitmq):
    base = AQXBase()
    base.connect()

    assert base._connection.is_open
    assert base._channel.is_open


def test_aqxbase_can_close(rabbitmq):
    base = AQXBase()
    base.connect()
    base.close()

    assert base._connection.is_closed