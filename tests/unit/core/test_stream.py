import os
import socket
import time

import pytest

from is_wire.core import Channel, Message, StreamSubscription, new_uuid
from is_wire.core import subscription as subscription_module

URI = os.getenv("WIRE_RABBITMQ_URI", "amqp://guest:guest@localhost:5672")


def _name(prefix):
    return f"{prefix}-{new_uuid():x}"


def _message(value):
    return Message(content=str(value).encode())


def _drain_until(channel, predicate, timeout=1.0):
    while not predicate():
        channel.connection.drain_events(timeout)


def test_stream_requires_a_group():
    channel = Channel(uri=URI)
    with pytest.raises(ValueError):
        StreamSubscription(channel, group="")
    channel.close()


def test_publish_stream_does_not_mutate_message_timeout():
    channel = Channel(uri=URI)
    topic = _name("stream-publish")
    stream = StreamSubscription(channel, group=_name("publisher"))
    stream.subscribe(topic)

    message = _message("payload")
    assert message.timeout is None
    channel.publish_stream(message, topic=topic)
    received = stream.consume(timeout=1.0)

    assert received.body == message.body
    assert received.timeout == 2.0
    assert message.timeout is None
    assert received.ack() is True
    assert received.ack() is False
    channel.close()


def test_stream_keeps_only_the_latest_pending_frame():
    channel = Channel(uri=URI)
    topic = _name("stream-latest")
    stream = StreamSubscription(channel, group=_name("latest"))
    stream.subscribe(topic)

    channel.publish_stream(_message(0), topic=topic)
    _drain_until(channel, lambda: bool(stream._stream_deliveries))
    channel.publish_stream(_message(1), topic=topic)
    channel.publish_stream(_message(2), topic=topic)
    # queue_declare faz uma ida e volta; portanto, as duas publicações
    # assíncronas foram aplicadas pelo broker antes da confirmação do quadro.
    channel._channel.queue_declare(queue=stream.name, passive=True)

    first = stream.consume(timeout=1.0)
    assert first.body == b"0"
    first.ack()
    latest = stream.consume(timeout=1.0)
    assert latest.body == b"2"
    latest.ack()

    with pytest.raises(socket.timeout):
        stream.consume(timeout=0.05)
    channel.close()


def test_same_group_load_balances_without_duplicate_delivery():
    channel = Channel(uri=URI)
    topic = _name("stream-workers")
    group = _name("detector")
    workers = [StreamSubscription(channel, group=group) for _ in range(3)]
    for worker in workers:
        worker.subscribe(topic)

    received = [0, 0, 0]
    for value in range(9):
        channel.publish_stream(_message(value), topic=topic)
        _drain_until(
            channel,
            lambda: any(worker._stream_deliveries for worker in workers),
        )
        index = next(
            index for index, worker in enumerate(workers) if worker._stream_deliveries
        )
        delivery = workers[index].consume(timeout=0)
        received[index] += 1
        delivery.ack()

    assert sum(received) == 9
    assert all(count > 0 for count in received)
    channel.close()


def test_different_groups_receive_independent_copies():
    channel = Channel(uri=URI)
    topic = _name("stream-fanout")
    groups = [StreamSubscription(channel, group=_name("stage")) for _ in range(3)]
    for group in groups:
        group.subscribe(topic)

    channel.publish_stream(_message("frame"), topic=topic)
    _drain_until(
        channel,
        lambda: all(group._stream_deliveries for group in groups),
    )
    messages = [group.consume(timeout=0) for group in groups]

    assert [message.body for message in messages] == [b"frame"] * 3
    for message in messages:
        message.ack()
    channel.close()


def test_stream_callback_error_is_not_requeued():
    channel = Channel(uri=URI)
    topic = _name("stream-error")
    stream = StreamSubscription(channel, group=_name("failing"))
    stream.subscribe(topic)
    channel.publish_stream(_message("bad-frame"), topic=topic)

    def fail(message):
        stream.stop()
        raise RuntimeError("cannot decode frame")

    stream.run(fail)
    stream._stopped.clear()
    with pytest.raises(socket.timeout):
        stream.consume(timeout=0.05)
    channel.close()


def test_stream_shutdown_rejects_a_callback_that_exceeds_grace_period(monkeypatch):
    monkeypatch.setattr(subscription_module, "STREAM_SHUTDOWN_TIMEOUT_SECONDS", 0.05)
    channel = Channel(uri=URI)
    topic = _name("stream-shutdown")
    stream = StreamSubscription(channel, group=_name("slow"))
    stream.subscribe(topic)
    channel.publish_stream(_message("slow-frame"), topic=topic)
    captured = []

    def slow(message):
        captured.append(message)
        stream.stop()
        time.sleep(0.2)

    stream.run(slow)

    assert len(captured) == 1
    assert captured[0].acknowledgeable is False
    channel.close()
