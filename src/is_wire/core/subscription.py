import logging
import signal
import socket
import threading
import time
from collections import deque
from threading import Event, Thread

import amqp

from .metrics import (
    MESSAGES_RECEIVED,
    RECEIVED_BYTES,
    STREAM_CALLBACK_ERRORS,
    STREAM_FRAME_AGE,
    STREAM_PROCESSING,
)
from .utils import consumer_id, now

QUEUE_EXPIRY_MILLISECONDS = 5 * 60 * 1000
STREAM_MESSAGE_TTL_MILLISECONDS = 2 * 1000
STREAM_SHUTDOWN_TIMEOUT_SECONDS = 5.0


class Subscription:
    def __init__(
        self,
        channel,
        name=None,
        *,
        auto_ack=True,
        prefetch=0,
        queue_arguments=None,
        _amqp_channel=None,
    ):
        self._parent = channel
        self._id = consumer_id()
        self._name = self._id if name is None else name
        self._anonymous = name is None
        self._topics = set()
        self._deliveries = deque()
        self._exchange = channel._exchange
        self._auto_ack = auto_ack
        self._prefetch = prefetch
        self._queue_arguments = dict(queue_arguments or {})
        if not self._anonymous:
            self._queue_arguments.setdefault("x-expires", QUEUE_EXPIRY_MILLISECONDS)
        self._on_message = self._enqueue
        self._channel = _amqp_channel or channel._channel
        self._channel.auto_decode = False
        self._declare()
        channel._register_subscription(self)

    def _declare(self):
        self._channel.queue_declare(
            queue=self._name,
            passive=False,
            durable=not self._anonymous,
            exclusive=self._anonymous,
            auto_delete=self._anonymous,
            arguments=self._queue_arguments or None,
        )
        self._channel.queue_bind(
            queue=self._name,
            exchange=self._exchange,
            routing_key=self._name,
        )
        if not self._auto_ack:
            self._channel.basic_qos(0, self._prefetch, False)
        self._channel.basic_consume(
            queue=self._name,
            callback=self._on_message,
            consumer_tag=self._id,
            no_local=False,
            no_ack=self._auto_ack,
            exclusive=False,
        )
        for topic in self._topics:
            self._channel.queue_bind(
                queue=self._name,
                exchange=self._exchange,
                routing_key=topic,
            )

    def _restore(self, channel):
        self._deliveries.clear()
        self._channel = channel
        self._declare()

    def _enqueue(self, delivery):
        self._deliveries.append(delivery)
        self._parent._delivery_ready(self)

    def subscribe(self, topic):
        if not isinstance(topic, str) or not topic:
            raise ValueError("topic must be a non-empty string")
        self._channel.queue_bind(
            queue=self._name,
            exchange=self._exchange,
            routing_key=topic,
        )
        self._topics.add(topic)

    def unsubscribe(self, topic):
        self._channel.queue_unbind(
            queue=self._name,
            exchange=self._exchange,
            routing_key=topic,
        )
        self._topics.remove(topic)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def topics(self):
        return self._topics

    @property
    def auto_ack(self):
        return self._auto_ack


class StreamSubscription(Subscription):
    """Assinatura agrupada do quadro mais recente para payloads binários em tempo real."""

    def __init__(self, channel, group):
        if not isinstance(group, str) or not group.strip():
            raise ValueError("stream group must be a non-empty string")
        self._group = group.strip()
        self._stream_deliveries = deque()
        self._stopped = Event()
        super().__init__(
            channel,
            name=f"is-wire.stream.{self._group}",
            auto_ack=False,
            prefetch=1,
            queue_arguments={
                "x-expires": QUEUE_EXPIRY_MILLISECONDS,
                "x-message-ttl": STREAM_MESSAGE_TTL_MILLISECONDS,
                "x-max-length": 1,
                "x-overflow": "drop-head",
            },
            _amqp_channel=channel.connection.channel(),
        )

    def _declare(self):
        self._on_message = self._stream_deliveries.append
        super()._declare()

    def _restore(self, channel):
        self._stream_deliveries.clear()
        self._channel = self._parent.connection.channel()
        self._channel.auto_decode = False
        self._declare()

    @property
    def group(self):
        return self._group

    def stop(self):
        self._stopped.set()

    def _settle(self, message, *, reject=False):
        try:
            if reject:
                message.reject(requeue=False)
            else:
                message.ack()
        except (amqp.exceptions.RecoverableConnectionError, OSError):
            # A confirmação pode não ter chegado ao broker. Não repita a
            # confirmação em um novo canal; deixe o RabbitMQ reentregar.
            self._parent._reconnect()

    def consume(self, timeout=None):
        from .wire.conversion import WireV1

        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be greater than or equal to zero")
        deadline = None if timeout is None else time.monotonic() + timeout

        while not self._stream_deliveries:
            if self._stopped.is_set():
                return None
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            try:
                self._parent.connection.drain_events(remaining)
            except TimeoutError:
                raise
            except (amqp.exceptions.RecoverableConnectionError, OSError) as error:
                if isinstance(error, socket.timeout):
                    raise
                self._parent._reconnect()
            if (
                deadline is not None
                and time.monotonic() >= deadline
                and not self._stream_deliveries
            ):
                raise TimeoutError()

        delivery = self._stream_deliveries.popleft()
        message = WireV1.from_amqp_message(delivery, acknowledgeable=True)
        MESSAGES_RECEIVED.labels(self._exchange, message.topic, "stream").inc()
        RECEIVED_BYTES.labels(self._exchange, message.topic, "stream").inc(len(message.body))
        STREAM_FRAME_AGE.labels(self._exchange, self._group).observe(
            max(0.0, now() - message.created_at)
        )
        return message

    def run(self, callback):
        if not callable(callback):
            raise TypeError("stream callback must be callable")
        self._stopped.clear()
        previous_handlers = {}

        def request_stop(signum, frame):
            del signum, frame
            self.stop()

        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, request_stop)

        try:
            while not self._stopped.is_set():
                try:
                    message = self.consume(timeout=1.0)
                except TimeoutError:
                    continue
                if message is None:
                    break
                begin = time.monotonic()
                completed = Event()
                outcome = {}

                def invoke_callback(
                    current_message=message,
                    current_outcome=outcome,
                    current_completed=completed,
                ):
                    try:
                        callback(current_message)
                    except BaseException as callback_error:
                        current_outcome["error"] = callback_error
                    finally:
                        current_completed.set()

                Thread(target=invoke_callback, daemon=True).start()
                shutdown_deadline = None
                while not completed.wait(0.1):
                    if self._stopped.is_set() and shutdown_deadline is None:
                        shutdown_deadline = time.monotonic() + STREAM_SHUTDOWN_TIMEOUT_SECONDS
                    if shutdown_deadline is not None and time.monotonic() >= shutdown_deadline:
                        STREAM_CALLBACK_ERRORS.labels(self._exchange, self._group).inc()
                        self._settle(message, reject=True)
                        logging.getLogger(__name__).warning(
                            "stream callback did not stop within five seconds; "
                            "frame rejected without requeue"
                        )
                        break
                else:
                    error = outcome.get("error")
                    if error is None:
                        self._settle(message)
                    else:
                        STREAM_CALLBACK_ERRORS.labels(self._exchange, self._group).inc()
                        self._settle(message, reject=True)
                        logging.getLogger(__name__).error(
                            "stream callback failed; frame rejected without requeue",
                            exc_info=(type(error), error, error.__traceback__),
                        )

                STREAM_PROCESSING.labels(self._exchange, self._group).observe(
                    time.monotonic() - begin
                )
                if not completed.is_set():
                    break
        except KeyboardInterrupt:
            self.stop()
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
