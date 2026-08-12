import pytest

from is_wire.core import Message, Status, StatusCode, ZipkinTracing
from is_wire.rpc import TracingInterceptor

exporter_module = pytest.importorskip(
    "opentelemetry.sdk.trace.export.in_memory_span_exporter"
)


def test_shared_provider_batches_spans_and_sets_service_resource():
    exporter = exporter_module.InMemorySpanExporter()
    tracing = ZipkinTracing(
        "camera-gateway",
        "http://zipkin:9411",
        service_instance_id="camera-5",
        sample_ratio=1.0,
        exporter=exporter,
    )

    with tracing.tracer().span("camera.frame") as span:
        span.set_attribute("camera.id", "5")

    assert tracing.force_flush()
    exported = exporter.get_finished_spans()
    assert len(exported) == 1
    assert exported[0].name == "camera.frame"
    assert exported[0].attributes["camera.id"] == "5"
    assert exported[0].resource.attributes["service.name"] == "camera-gateway"
    assert exported[0].resource.attributes["service.instance.id"] == "camera-5"
    assert tracing.endpoint == "http://zipkin:9411/api/v2/spans"
    tracing.shutdown()


def test_shared_provider_preserves_message_parent_context():
    exporter = exporter_module.InMemorySpanExporter()
    tracing = ZipkinTracing("camera-gateway", sample_ratio=1.0, exporter=exporter)
    message = Message(content=b"frame")

    with tracing.tracer().span("camera.publish") as parent:
        message.inject_tracing(parent)

    with tracing.tracer(message.extract_tracing()).span("detector.consume"):
        pass

    assert tracing.force_flush()
    parent_span, child_span = exporter.get_finished_spans()
    assert child_span.context.trace_id == parent_span.context.trace_id
    assert child_span.parent.span_id == parent_span.context.span_id
    tracing.shutdown()


def test_unsampled_parent_decision_is_propagated_to_downstream_service():
    gateway_exporter = exporter_module.InMemorySpanExporter()
    detector_exporter = exporter_module.InMemorySpanExporter()
    gateway = ZipkinTracing("camera-gateway", sample_ratio=0.0, exporter=gateway_exporter)
    detector = ZipkinTracing("person-detector", sample_ratio=1.0, exporter=detector_exporter)
    message = Message(content=b"frame")

    with gateway.tracer().span("camera.frame") as parent:
        message.inject_tracing(parent)

    extracted = message.extract_tracing()
    assert extracted.sampled is False

    with detector.tracer(span_context=extracted).span("frame_pipeline"):
        pass

    assert gateway.force_flush()
    assert detector.force_flush()
    assert gateway_exporter.get_finished_spans() == ()
    assert detector_exporter.get_finished_spans() == ()
    gateway.shutdown()
    detector.shutdown()


def test_rpc_interceptor_uses_the_shared_provider():
    exporter = exporter_module.InMemorySpanExporter()
    tracing = ZipkinTracing("camera-gateway", sample_ratio=1.0, exporter=exporter)
    interceptor = TracingInterceptor(tracing=tracing)
    request = Message(content=b"request")
    request.topic = "CameraGateway.5.GetConfig"
    reply = Message(content=b"reply")
    reply.status = Status(StatusCode.OK)

    class Context:
        addons = {}

    context = Context()
    context.request = request
    context.reply = reply
    interceptor.before_call(context)
    interceptor.after_call(context)

    assert tracing.force_flush()
    assert exporter.get_finished_spans()[0].name == "GetConfig"
    assert reply.extract_tracing().trace_id is not None
    tracing.shutdown()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"service_name": ""},
        {"service_name": "camera", "endpoint": "zipkin:9411"},
        {"service_name": "camera", "sample_ratio": -0.1},
        {"service_name": "camera", "sample_ratio": 1.1},
    ],
)
def test_invalid_zipkin_configuration_is_rejected(kwargs):
    exporter = exporter_module.InMemorySpanExporter()
    with pytest.raises(ValueError):
        ZipkinTracing(exporter=exporter, **kwargs)


def test_closed_provider_rejects_new_tracers():
    tracing = ZipkinTracing(
        "camera-gateway",
        exporter=exporter_module.InMemorySpanExporter(),
    )
    tracing.shutdown()

    with pytest.raises(RuntimeError, match="closed"):
        tracing.tracer()
