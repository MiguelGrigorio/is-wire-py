from ..core import Logger, Tracer
from ..rpc import Interceptor


def service_name(context):
    return context.request.topic.split(".")[-1]


class TracingInterceptor(Interceptor):
    def __init__(self, exporter=None, span_namer=service_name):
        self.log = Logger(name="TracingInterceptor")
        self.exporter = exporter
        self.namer = span_namer

    def before_call(self, context):
        tracer = Tracer(
            self.exporter,
            span_context=context.request.extract_tracing(),
        )
        span = tracer.start_span(name=self.namer(context))
        context.addons["tracer"] = tracer
        context.addons["tracing_interceptor.span"] = span

    def after_call(self, context):
        tracer = context.addons["tracer"]
        span = context.addons["tracing_interceptor.span"]
        status = context.reply.status
        if not status.ok():
            span.set_attribute("reply_to", context.request.reply_to or "")
            span.set_attribute("status_code", status.code.name)
            span.set_attribute("status_why", status.why)

        context.reply.inject_tracing(span)
        tracer.end_span()
