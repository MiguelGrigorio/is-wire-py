import json
from pathlib import Path

import amqp

from is_wire.core import ContentType, Message, Status, StatusCode, now
from is_wire.core.wire.conversion import WireV1

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures"


def test_amqp_conversion():
    sent = Message()
    sent.created_at = int(now() * 1000) / 1000.0
    sent.reply_to = "reply_to"
    sent.subscription_id = "subscription_id"
    sent.content_type = ContentType.JSON
    sent.body = '{"field":"value"}'.encode('latin1')
    sent.topic = "MyTopic"
    sent.status = Status(
        code=StatusCode.FAILED_PRECONDITION,
        why="Bad Args...",
    )
    sent.metadata = {
        'x-b3-sampled': '1',
        'x-b3-traceid': 'f047c6f208eb36ab',
        'x-b3-flags': '0',
        'x-b3-spanid': 'ef81a2f9c261473d',
        'x-b3-parentspanid': '0000000000000000'
    }

    body = sent.body

    amqp_message = amqp.Message(
        channel=None, body=body, **WireV1.to_amqp_properties(sent))

    amqp_message.delivery_info = {
        "routing_key": sent.topic,
        "consumer_tag": sent.subscription_id,
    }

    received = WireV1.from_amqp_message(amqp_message)
    print(sent.__str__(), received.__str__())
    assert str(sent) == str(received)
    assert sent.created_at == received.created_at
    assert sent.reply_to == received.reply_to
    assert sent.subscription_id == received.subscription_id
    assert sent.content_type == received.content_type
    assert sent.body == received.body
    assert sent.status == received.status
    assert sent.topic == received.topic
    assert sent.correlation_id == received.correlation_id
    assert sent.timeout == received.timeout
    assert sent.metadata == received.metadata


def test_wire_121_golden_body_and_properties_are_unchanged():
    body = (FIXTURES / "wire_v1_body.bin").read_bytes()
    expected = json.loads((FIXTURES / "wire_v1_properties.json").read_text())
    message = Message(body)
    message.created_at = 1700000000.123
    message.content_type = ContentType.PROTOBUF
    message.correlation_id = 0x0123456789ABCDEF
    message.reply_to = "reply.queue"
    message.timeout = 2
    message.metadata = {
        "x-b3-traceid": "f047c6f208eb36ab",
        "custom": "value",
    }
    message.status = Status(StatusCode.FAILED_PRECONDITION, "bad request")

    assert message.body == body
    assert WireV1.to_amqp_properties(message) == expected
