import os
import socket
import ssl
from collections import deque
from unittest.mock import Mock

import pytest
from google.protobuf.struct_pb2 import Struct

from is_wire.core import Channel, Message, Subscription, now
from is_wire.core.channel import LARGE_STREAM_PAYLOAD
from is_wire.core.subscription import Subscription as CoreSubscription

URI = os.getenv('WIRE_RABBITMQ_URI', 'amqp://guest:guest@localhost:5672')
EXCHANGE = os.getenv('WIRE_DEFAULT_EXCHANGE', 'is')


def _channel_parameters(uri, **overrides):
    channel = Channel.__new__(Channel)
    channel._uri = uri
    channel._heartbeat = overrides.get("heartbeat", 30)
    channel._connect_timeout = overrides.get("connect_timeout", 5.0)
    channel._read_timeout = overrides.get("read_timeout")
    channel._write_timeout = overrides.get("write_timeout")
    channel._ssl_options = overrides.get("ssl_options")
    return channel._connection_parameters()


def test_channel():
    channel = Channel(uri=URI, exchange=EXCHANGE)
    subscription = Subscription(channel)
    subscription.subscribe("MyTopic.Sub.Sub")

    struct = Struct()
    struct.fields["value"].number_value = 666.0

    sent = Message(struct)
    sent.reply_to = subscription
    sent.created_at = int(1000 * now()) / 1000.0
    sent.timeout = 1.0
    sent.topic = "MyTopic.Sub.Sub"

    channel.publish(message=sent)
    received = channel.consume(timeout=1.0)

    assert sent.reply_to == received.reply_to
    assert sent.subscription_id == received.subscription_id
    assert sent.content_type == received.content_type
    assert sent.body == received.body
    assert sent.status == received.status
    assert sent.topic == received.topic
    assert sent.correlation_id == received.correlation_id
    assert sent.timeout == received.timeout
    assert sent.metadata == received.metadata
    assert sent.created_at == received.created_at
    assert str(sent) == str(received)

    struct2 = received.unpack(Struct)
    assert str(struct) == str(struct2)
    assert struct == struct2

    channel.close()


@pytest.mark.parametrize("size", [0, 1e4])
def test_body(size):
    channel = Channel(uri=URI, exchange=EXCHANGE)

    subscription = Subscription(channel)
    subscription.subscribe("MyTopic.Sub.Sub")

    sent = Message()
    sent.reply_to = subscription
    sent.topic = "MyTopic.Sub.Sub"
    sent.body = bytes(bytearray(range(256)) * int(size))

    channel.publish(message=sent)
    received = channel.consume(timeout=1.0)

    assert repr(sent.body) == repr(received.body)
    assert sent.body == received.body

    channel.close()


def test_negative_timeout():
    channel = Channel(uri=URI, exchange=EXCHANGE)
    with pytest.raises(ValueError):
        channel.consume(timeout=-1e-10)
    with pytest.raises(socket.timeout):
        channel.consume(timeout=0)
    channel.close()


def test_empty_topic():
    channel = Channel(uri=URI, exchange=EXCHANGE)
    message = Message(content="body".encode('latin'))

    with pytest.raises(RuntimeError):
        channel.publish(message)

    with pytest.raises(RuntimeError):
        channel.publish(message, topic="")

    subscription = Subscription(channel)
    channel.publish(message, topic=subscription.name)
    recv = channel.consume(timeout=1.0)
    assert recv.body == message.body

    message.topic = subscription.name
    channel.publish(message)
    recv = channel.consume(timeout=1.0)
    assert recv.body == message.body
    channel.close()


def test_multi_subscription():
    channel = Channel(uri=URI, exchange=EXCHANGE)
    message = Message()
    subscription1 = Subscription(channel)
    subscription2 = Subscription(channel)

    channel.publish(message, topic=subscription1.name)
    recv = channel.consume(timeout=1.0)
    assert recv.subscription_id == subscription1.name

    channel.publish(message, topic=subscription2.name)
    recv = channel.consume(timeout=1.0)
    assert recv.subscription_id == subscription2.name
    channel.close()


def test_amqps_requires_certificate_and_hostname_validation():
    parameters = _channel_parameters(
        "amqps://user:p%40ss@rabbit.example:5671/%2F",
        ssl_options={"ca_certs": "/tmp/ca.pem"},
    )

    assert parameters["userid"] == "user"
    assert parameters["password"] == "p@ss"
    assert parameters["virtual_host"] == "/"
    assert parameters["ssl"]["cert_reqs"] == ssl.CERT_REQUIRED
    assert parameters["ssl"]["server_hostname"] == "rabbit.example"

    with pytest.raises(ValueError):
        _channel_parameters(
            "amqps://rabbit.example",
            ssl_options={"cert_reqs": ssl.CERT_NONE},
        )


def test_invalid_uri_scheme_is_rejected_before_connecting():
    with pytest.raises(ValueError):
        _channel_parameters("http://rabbit.example")


def test_anonymous_subscription_uses_fresh_queue_when_restored(monkeypatch):
    subscription = CoreSubscription.__new__(CoreSubscription)
    subscription._anonymous = True
    subscription._id = "stable-consumer-id"
    subscription._name = "old-exclusive-queue"
    subscription._deliveries = deque([object()])
    subscription._declare = Mock()
    new_channel = Mock()
    monkeypatch.setattr(
        "is_wire.core.subscription.consumer_id", lambda: "new-exclusive-queue"
    )

    subscription._restore(new_channel)

    assert subscription.id == "stable-consumer-id"
    assert subscription.name == "new-exclusive-queue"
    assert subscription._channel is new_channel
    assert not subscription._deliveries
    subscription._declare.assert_called_once_with()


def test_named_subscription_keeps_queue_name_when_restored(monkeypatch):
    subscription = CoreSubscription.__new__(CoreSubscription)
    subscription._anonymous = False
    subscription._id = "consumer-id"
    subscription._name = "durable-service-queue"
    subscription._deliveries = deque()
    subscription._declare = Mock()
    consumer_id_mock = Mock()
    monkeypatch.setattr("is_wire.core.subscription.consumer_id", consumer_id_mock)

    subscription._restore(Mock())

    assert subscription.name == "durable-service-queue"
    consumer_id_mock.assert_not_called()


def test_large_stream_payload_warns_but_is_published_without_compression():
    channel = Channel.__new__(Channel)
    channel._exchange = "is"
    channel._channel = Mock()
    payload = b"x" * (LARGE_STREAM_PAYLOAD + 1)
    message = Message(content=payload)

    with pytest.warns(RuntimeWarning, match="larger than 16 MiB"):
        channel.publish_stream(message, topic="Camera.0.Frame")

    published = channel._channel.basic_publish.call_args.args[0]
    assert published.body is payload
    assert published.properties["delivery_mode"] == 1
    assert published.properties["expiration"] == "2000"
