import contextlib
import secrets
import warnings
from datetime import datetime, timezone
from types import SimpleNamespace

from opentelemetry import trace
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
    set_span_in_context,
)

from .propagation import TracingContext

try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
except ImportError:  # The SDK intentionally lives in the optional tracing extra.
    TracerProvider = None
    SimpleSpanProcessor = None
    SpanExportResult = None
    SpanExporter = object


class _SpanAdapter:
    def __init__(self, span=None, trace_id=None, span_id=None):
        self._span = span
        if span is not None:
            context = span.get_span_context()
            trace_id = f"{context.trace_id:032x}"
            span_id = f"{context.span_id:016x}"
        self.context_tracer = SimpleNamespace(trace_id=trace_id)
        self.span_id = span_id
        self._attributes = {}

    def get_span_context(self):
        if self._span is not None:
            return self._span.get_span_context()
        return SpanContext(
            trace_id=int(self.context_tracer.trace_id, 16),
            span_id=int(self.span_id, 16),
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

    def set_attribute(self, key, value):
        if self._span is not None:
            self._span.set_attribute(key, value)
        else:
            self._attributes[key] = value
        return self

    def add_attribute(self, key, value):
        return self.set_attribute(key, value)

    def end(self):
        if self._span is not None:
            self._span.end()


class _TracerCompat:
    def __init__(self, delegate):
        self._delegate = delegate
        self.span_context = SimpleNamespace(trace_id=None)

    def __getattr__(self, name):
        return getattr(self._delegate, name)


class _LegacyExporterAdapter(SpanExporter):
    def __init__(self, exporter):
        self._exporter = exporter

    def export(self, spans):
        from opencensus.trace.span_context import SpanContext as OpenCensusSpanContext
        from opencensus.trace.span_data import SpanData

        converted = []
        for span in spans:
            context = span.context
            parent = span.parent
            oc_context = OpenCensusSpanContext(
                trace_id=f"{context.trace_id:032x}",
                span_id=f"{context.span_id:016x}",
            )
            converted.append(
                SpanData(
                    name=span.name,
                    context=oc_context,
                    span_id=f"{context.span_id:016x}",
                    parent_span_id=f"{parent.span_id:016x}" if parent else None,
                    attributes=dict(span.attributes or {}),
                    start_time=_timestamp(span.start_time),
                    end_time=_timestamp(span.end_time),
                    child_span_count=0,
                    stack_trace=None,
                    annotations=None,
                    message_events=None,
                    links=None,
                    status=None,
                    same_process_as_parent_span=True,
                    span_kind=0,
                )
            )
        self._exporter.export(converted)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        shutdown = getattr(self._exporter, "shutdown", None)
        if shutdown is not None:
            shutdown()


def _timestamp(nanoseconds):
    if nanoseconds is None:
        return None
    return datetime.fromtimestamp(nanoseconds / 1_000_000_000, timezone.utc).isoformat()


class Tracer:
    """OpenTelemetry-backed facade preserving the original is-wire API."""

    def __init__(self, exporter=None, span_context=None):
        self._parent_context = _otel_parent_context(span_context)
        self._active = []
        self._provider = None

        if TracerProvider is not None:
            self._provider = TracerProvider()
            if exporter is not None:
                if exporter.__class__.__module__.startswith("opencensus"):
                    warnings.warn(
                        "OpenCensus exporters are deprecated; "
                        "migrate to an OpenTelemetry exporter",
                        DeprecationWarning,
                        stacklevel=2,
                    )
                    exporter = _LegacyExporterAdapter(exporter)
                self._provider.add_span_processor(SimpleSpanProcessor(exporter))
            self._otel_tracer = self._provider.get_tracer("is-wire-sea", "1.3.0")
        else:
            if exporter is not None:
                raise RuntimeError(
                    "An exporter requires the 'is-wire-sea[tracing]' optional dependency"
                )
            self._otel_tracer = trace.get_tracer("is-wire-sea", "1.3.0")
        self.tracer = _TracerCompat(self._otel_tracer)

    @contextlib.contextmanager
    def span(self, name="span"):
        if self._provider is None:
            adapter = _local_span(self._parent_context)
            self.tracer.span_context.trace_id = adapter.context_tracer.trace_id
            yield adapter
            return

        with self._otel_tracer.start_as_current_span(name, context=self._parent_context) as span:
            adapter = _SpanAdapter(span=span)
            self.tracer.span_context.trace_id = adapter.context_tracer.trace_id
            yield adapter

    def start_span(self, name="span"):
        if self._provider is None:
            adapter = _local_span(self._parent_context)
        else:
            adapter = _SpanAdapter(
                span=self._otel_tracer.start_span(name, context=self._parent_context)
            )
        self.tracer.span_context.trace_id = adapter.context_tracer.trace_id
        self._active.append(adapter)
        return adapter

    def end_span(self):
        if not self._active:
            return None
        span = self._active.pop()
        span.end()
        return span

    def shutdown(self):
        if self._provider is not None:
            self._provider.shutdown()


def _local_span(parent_context):
    if parent_context is not None:
        parent = trace.get_current_span(parent_context).get_span_context()
        trace_id = f"{parent.trace_id:032x}"
    else:
        trace_id = secrets.token_hex(16)
    return _SpanAdapter(trace_id=trace_id, span_id=secrets.token_hex(8))


def _otel_parent_context(span_context):
    if span_context is None:
        return None
    if isinstance(span_context, TracingContext):
        trace_id = span_context.trace_id.rjust(32, "0")
        context = SpanContext(
            trace_id=int(trace_id, 16),
            span_id=int(span_context.span_id, 16),
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED if span_context.sampled else 0),
            trace_state=TraceState(),
        )
        return set_span_in_context(NonRecordingSpan(context))
    if isinstance(span_context, SpanContext):
        return set_span_in_context(NonRecordingSpan(span_context))
    return span_context
