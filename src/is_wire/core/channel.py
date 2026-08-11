import logging
import socket
import ssl
import time
import warnings
from collections import deque
from urllib.parse import unquote, urlparse

import amqp

from .metrics import (
    MESSAGES_PUBLISHED,
    MESSAGES_RECEIVED,
    PUBLISHED_BYTES,
    RECEIVED_BYTES,
    RECONNECTIONS,
)
from .wire.conversion import WireV1

STREAM_EXPIRATION_SECONDS = 2.0
LARGE_STREAM_PAYLOAD = 16 * 1024 * 1024


class Channel:
    def __init__(
        self,
        uri="amqp://guest:guest@localhost:5672",
        exchange="is",
        *,
        heartbeat=30,
        connect_timeout=5.0,
        read_timeout=None,
        write_timeout=None,
        ssl_options=None,
        reconnect=True,
    ):
        self._uri = uri
        self._exchange = exchange
        self._heartbeat = heartbeat
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._ssl_options = ssl_options
        self._reconnect_enabled = reconnect
        self._closed = False
        self._ready_subscriptions = deque()
        self._subscriptions_by_id = {}
        self.subscriptions = []
        self.connection = None
        self._channel = None
        self._connect(restore=False)

    def _connection_parameters(self):
        url = urlparse(self._uri)
        if url.scheme not in {"amqp", "amqps"}:
            raise ValueError("RabbitMQ URI must use the amqp or amqps scheme")

        hostname = url.hostname or "localhost"
        port = url.port or (5671 if url.scheme == "amqps" else 5672)
        ssl_config = False
        if url.scheme == "amqps":
            ssl_config = {
                "cert_reqs": ssl.CERT_REQUIRED,
                "server_hostname": hostname,
            }
            if self._ssl_options:
                if self._ssl_options.get("cert_reqs", ssl.CERT_REQUIRED) != ssl.CERT_REQUIRED:
                    raise ValueError("amqps requires certificate validation")
                ssl_config.update(self._ssl_options)

        return {
            "host": f"{hostname}:{port}",
            "userid": unquote(url.username or "guest"),
            "password": unquote(url.password or "guest"),
            "virtual_host": "/" if not url.path or url.path == "/" else unquote(url.path[1:]),
            "connect_timeout": self._connect_timeout,
            "read_timeout": self._read_timeout,
            "write_timeout": self._write_timeout,
            "heartbeat": self._heartbeat,
            "ssl": ssl_config,
        }

    def _connect(self, *, restore):
        self.connection = amqp.Connection(**self._connection_parameters())
        self.connection.connect()
        self._channel = self.connection.channel()
        self._channel.auto_decode = False
        self._channel.exchange_declare(
            exchange=self._exchange,
            type="topic",
            durable=False,
            auto_delete=False,
        )

        if restore:
            self._ready_subscriptions.clear()
            for subscription in tuple(self.subscriptions):
                subscription._restore(self._channel)

    def _reconnect(self):
        if self._closed or not self._reconnect_enabled:
            raise ConnectionError("RabbitMQ connection is closed")

        delay = 1.0
        while not self._closed:
            RECONNECTIONS.labels(self._exchange).inc()
            try:
                if self.connection is not None:
                    try:
                        self.connection.close()
                    except Exception:
                        pass
                self._connect(restore=True)
                return
            except (amqp.exceptions.ConnectionError, OSError):
                logging.getLogger(__name__).warning(
                    "RabbitMQ reconnect failed; retrying in %.1fs", delay
                )
                time.sleep(delay)
                delay = min(delay * 2, 30.0)

        raise ConnectionError("RabbitMQ connection was closed while reconnecting")

    def _register_subscription(self, subscription):
        self.subscriptions.append(subscription)
        self._subscriptions_by_id[subscription.id] = subscription

    def _delivery_ready(self, subscription):
        self._ready_subscriptions.append(subscription.id)

    def _convert_delivery(self, delivery, subscription):
        acknowledgeable = not subscription.auto_ack
        message = WireV1.from_amqp_message(delivery, acknowledgeable=acknowledgeable)
        MESSAGES_RECEIVED.labels(self._exchange, message.topic, "standard").inc()
        RECEIVED_BYTES.labels(self._exchange, message.topic, "standard").inc(len(message.body))
        return message

    def publish(self, message, topic=None):
        """Publish a message without retrying it after an uncertain failure."""
        routing_key = message.topic if topic is None else topic
        if not routing_key:
            raise RuntimeError("Trying to publish message without topic")

        amqp_message = amqp.Message(
            body=message.body,
            channel=self._channel,
            **WireV1.to_amqp_properties(message),
        )
        self._channel.basic_publish(
            amqp_message,
            exchange=self._exchange,
            routing_key=routing_key,
            immediate=False,
            mandatory=False,
        )
        MESSAGES_PUBLISHED.labels(self._exchange, routing_key, "standard").inc()
        PUBLISHED_BYTES.labels(self._exchange, routing_key, "standard").inc(len(message.body))

    def publish_stream(self, message, topic=None, *, expiration=STREAM_EXPIRATION_SECONDS):
        """Publish a non-persistent, short-lived real-time payload."""
        routing_key = message.topic if topic is None else topic
        if not routing_key:
            raise RuntimeError("Trying to publish stream message without topic")
        if expiration <= 0:
            raise ValueError("stream expiration must be greater than zero")
        if len(message.body) > LARGE_STREAM_PAYLOAD:
            warnings.warn(
                f"stream payload is larger than 16 MiB ({len(message.body)} bytes)",
                RuntimeWarning,
                stacklevel=2,
            )

        amqp_message = amqp.Message(
            body=message.body,
            channel=self._channel,
            **WireV1.to_amqp_properties(
                message,
                expiration=expiration,
                delivery_mode=1,
            ),
        )
        self._channel.basic_publish(
            amqp_message,
            exchange=self._exchange,
            routing_key=routing_key,
            immediate=False,
            mandatory=False,
        )
        MESSAGES_PUBLISHED.labels(self._exchange, routing_key, "stream").inc()
        PUBLISHED_BYTES.labels(self._exchange, routing_key, "stream").inc(len(message.body))

    def consume(self, timeout=None):
        """Block until a legacy subscription delivery is available."""
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be greater than or equal to zero")

        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._ready_subscriptions:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                self.connection.drain_events(remaining)
            except TimeoutError:
                raise
            except (amqp.exceptions.RecoverableConnectionError, OSError) as error:
                if isinstance(error, socket.timeout):
                    raise
                self._reconnect()
            if (
                deadline is not None
                and time.monotonic() >= deadline
                and not self._ready_subscriptions
            ):
                raise TimeoutError()
        subscription_id = self._ready_subscriptions.popleft()
        subscription = self._subscriptions_by_id[subscription_id]
        return self._convert_delivery(subscription._deliveries.popleft(), subscription)

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self.connection is not None:
            try:
                self.connection.close()
            except (amqp.exceptions.ConnectionError, OSError):
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
