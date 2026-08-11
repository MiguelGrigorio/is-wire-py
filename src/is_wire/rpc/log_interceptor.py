from ..core import Logger, StatusCode, now
from ..rpc import Interceptor


class LogInterceptor(Interceptor):

    def __init__(self):
        self.log = Logger(name='LogInterceptor')

    def before_call(self, context):
        context.addons["log_interceptor.started_at"] = now()

    def after_call(self, context):
        took = now() - context.addons["log_interceptor.started_at"]
        status = context.reply.status
        if status.ok():
            self.log.info("took={}s, code={}", took, status.code.name)
        elif status.code == StatusCode.INTERNAL_ERROR:
            self.log.error('took={}s status={} request={}', took, status,
                           context.request.short_string())
        else:
            self.log.warn('took={}s status={} request={}', took, status,
                          context.request.short_string())
