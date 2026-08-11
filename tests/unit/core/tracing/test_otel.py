import pytest

from is_wire.core import Tracer
from is_wire.core.tracing.propagation import TracingContext

trace_export = pytest.importorskip("opentelemetry.sdk.trace.export")


def test_otel_exporter_receives_a_completed_span():
    exporter_module = pytest.importorskip("opentelemetry.sdk.trace.export.in_memory_span_exporter")
    exporter = exporter_module.InMemorySpanExporter()
    tracer = Tracer(exporter=exporter)

    with tracer.span("camera.receive") as span:
        span.set_attribute("camera", "front")
    tracer.shutdown()

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "camera.receive"
    assert spans[0].attributes["camera"] == "front"


def test_otel_accepts_a_64_bit_b3_parent_trace_id():
    exporter_module = pytest.importorskip("opentelemetry.sdk.trace.export.in_memory_span_exporter")
    exporter = exporter_module.InMemorySpanExporter()
    parent = TracingContext(
        trace_id="f047c6f208eb36ab",
        span_id="ef81a2f9c261473d",
    )
    tracer = Tracer(exporter=exporter, span_context=parent)

    with tracer.span("rpc"):
        pass
    tracer.shutdown()

    exported = exporter.get_finished_spans()[0]
    assert exported.context.trace_id == int(parent.trace_id, 16)
