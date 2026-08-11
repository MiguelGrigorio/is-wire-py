import time

from prometheus_client import Counter, Histogram, start_http_server

from ..core import Logger
from ..rpc import Interceptor

RPC_DURATION = Histogram(
    "is_wire_rpc_duration_seconds",
    "RPC handler duration in seconds",
    ("service", "status_code"),
)
RPC_REQUESTS = Counter(
    "is_wire_rpc_requests_total",
    "RPC requests processed",
    ("service", "status_code"),
)


class MetricsInterceptor(Interceptor):
    def __init__(self):
        self.log = Logger(name="MetricsInterceptor")
        # Preserve the public attributes while exposing the corrected metric types.
        self.duration = RPC_DURATION
        self.count = RPC_REQUESTS

    def start_server(self, port=8000):
        start_http_server(port)

    def before_call(self, context):
        context.addons["metrics_interceptor.started_at"] = time.monotonic()

    def after_call(self, context):
        took = time.monotonic() - context.addons["metrics_interceptor.started_at"]
        topic = context.request.topic
        code = context.reply.status.code.name
        self.duration.labels(topic, code).observe(took)
        self.count.labels(topic, code).inc()
