"""Contains some general tools used by asyncqx"""
import re


# https://stackoverflow.com/questions/50679145/how-to-match-the-routing-key-with-binding-pattern-for-rabbitmq-topic-exchange-us
def amqp_match(key: str, pattern: str) -> bool:
    """Checks if a string matches a pattern used by RabbitMQ topic exchange."""
    if key == pattern:
        return True
    replaced = pattern.replace(r'*', r'([^.]+)').replace(r'#', r'([^.]+.?)+')
    regex_string = f"^{replaced}$"
    match = re.search(regex_string, key)
    return match is not None
