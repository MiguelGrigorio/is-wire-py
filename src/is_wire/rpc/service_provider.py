import signal
import threading
import traceback
from threading import Event

import amqp
from google.protobuf.json_format import ParseError
from google.protobuf.message import DecodeError

from ..core import Channel, Logger, Status, StatusCode, Subscription
from ..core.utils import assert_type
from .context import Context


class ServiceProvider:
    log = Logger("ServiceProvider", Logger.DEBUG)

    def __init__(self, channel):
        assert_type(channel, Channel, "channel")
        self._channel = channel
        self._services = {}
        self._interceptors_before = []
        self._interceptors_after = []
        self._subscriptions = []
        self._stopped = Event()

    def delegate(self, topic, function, request_type, reply_type):
        """Vincula uma função a um tópico.

        Ela será chamada a cada mensagem recebida nesse tópico.
        """
        assert_type(topic, str, "topic")
        if any(topic == s.name for s in self._subscriptions):
            raise RuntimeError(
                f"Service on topic '{topic}' was already delegated")

        self.log.debug("New service registered '{}'", topic)
        subscription = Subscription(
            self._channel,
            name=topic,
            auto_ack=False,
            prefetch=16,
        )
        self._subscriptions.append(subscription)
        wrapped = self.wrap(function, request_type, reply_type)
        self._services[subscription.id] = wrapped

    def add_interceptor(self, interceptor):
        """Adiciona um interceptor ao provider.

        Interceptors permitem executar funções antes e depois do handler do
        serviço. O objeto deve implementar os métodos ``before_call`` e
        ``after_call``.
        """
        itype = type(interceptor)
        if not hasattr(itype, "before_call") and \
           not hasattr(itype, "after_call"):
            raise TypeError("Interceptors must implement the Interceptor"
                            "concept or derive from the Interceptor class")
        self._interceptors_before.append(interceptor.before_call)
        self._interceptors_after.append(interceptor.after_call)

    def should_serve(self, message):
        return message.subscription_id in self._services

    def serve(self, message):
        """Tenta atender a mensagem; lança RuntimeError quando isso não é possível."""
        try:
            service = self._services[message.subscription_id]
        except KeyError as error:
            why = f"Cannot serve message with subscription_id='{message.subscription_id}'"
            raise RuntimeError(why) from error

        reply, timeouted = service(message)
        if reply.has_topic() and not timeouted:
            self._channel.publish(reply)
        message.ack()

    def run(self):
        """Bloqueia a thread atual enquanto escuta requisições."""
        self.log.info("Listening for requests")
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
                    message = self._channel.consume(timeout=1.0)
                except TimeoutError:
                    continue
                if self.should_serve(message):
                    try:
                        self.serve(message)
                    except (amqp.exceptions.RecoverableConnectionError, OSError):
                        # A requisição permanece sem confirmação e o RabbitMQ
                        # a reentregará depois que a conexão for restaurada.
                        self._channel._reconnect()
        except KeyboardInterrupt:
            self.stop()
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)

    def stop(self):
        self._stopped.set()

    def wrap(self, function, request_type, reply_type):

        def safe_call(*args):
            try:
                result = function(*args)
                assert_type(result, (Status, reply_type), "function result")
                return result
            except Exception:
                return Status(
                    code=StatusCode.INTERNAL_ERROR,
                    why=f"Service throwed exception:\n{traceback.format_exc()}",
                )

        def run_interceptors(interceptors, *args):
            for interceptor in interceptors:
                try:
                    interceptor(*args)
                except Exception:
                    trace = traceback.format_exc()
                    self.log.error("Interceptor throwed exception:\n{}", trace)

        def wrapper(request):
            reply = request.create_reply()
            context = Context(request, reply)

            run_interceptors(self._interceptors_before, context)

            if not request.deadline_exceeded():
                try:
                    arg = request.unpack(request_type)
                    result = safe_call(arg, context)
                    if isinstance(result, Status):
                        reply.status = result
                    else:
                        reply.pack(result)
                        reply.status = Status(code=StatusCode.OK)
                except (DecodeError, ParseError):
                    why = (
                        f"Expected request type '{request_type.DESCRIPTOR.full_name}' "
                        "but received something else"
                    )
                    reply.status = Status(StatusCode.FAILED_PRECONDITION, why)
                except Exception:
                    trace = traceback.format_exc()
                    self.log.error("Unexpected error\n{}", trace)
                    reply.status = Status(StatusCode.INTERNAL_ERROR, trace)

            timeouted = request.deadline_exceeded()
            if timeouted:
                reply.status = Status(StatusCode.DEADLINE_EXCEEDED)

            run_interceptors(self._interceptors_after, context)

            return reply, timeouted

        return wrapper
