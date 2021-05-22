from asyncqx.tools import amqp_match


def test_idempotency():
    assert amqp_match('event', 'event')
    assert amqp_match('some.event', 'some.event')
    assert amqp_match('some.other.event', 'some.other.event')

def test_star_wildcard():
    assert amqp_match('some.event', 'some.*')
    assert amqp_match('some.other', 'some.*')
    assert not amqp_match('some.other.event', 'some.*')

def test_hash_wildcard():
    assert amqp_match('some.event', 'some.#')
    assert amqp_match('some.other.event', 'some.#')