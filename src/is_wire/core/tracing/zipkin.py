from urllib.parse import urlparse


class ZipkinTracing:
    """Provider OpenTelemetry compartilhado e agrupado que exporta spans para o Zipkin."""

    def __init__(
        self,
        service_name,
        endpoint="http://localhost:9411/api/v2/spans",
        *,
        service_instance_id=None,
        sample_ratio=0.1,
        resource_attributes=None,
        exporter=None,
    ):
        if not isinstance(service_name, str) or not service_name.strip():
            raise ValueError("service_name must be a non-empty string")
        if not 0.0 <= sample_ratio <= 1.0:
            raise ValueError("sample_ratio must be between zero and one")

        endpoint = _normalize_endpoint(endpoint)

        try:
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
        except ImportError as error:
            raise RuntimeError(
                "Zipkin tracing requires the 'is-wire-sea[zipkin]' optional dependency"
            ) from error

        attributes = dict(resource_attributes or {})
        attributes["service.name"] = service_name.strip()
        if service_instance_id is not None:
            attributes["service.instance.id"] = str(service_instance_id)

        if exporter is None:
            try:
                from opentelemetry.exporter.zipkin.json import ZipkinExporter
            except ImportError as error:
                raise RuntimeError(
                    "Zipkin tracing requires the 'is-wire-sea[zipkin]' optional dependency"
                ) from error
            exporter = ZipkinExporter(endpoint=endpoint)

        self.endpoint = endpoint
        self.service_name = service_name.strip()
        self._provider = TracerProvider(
            resource=Resource.create(attributes),
            sampler=ParentBased(TraceIdRatioBased(sample_ratio)),
        )
        self._provider.add_span_processor(BatchSpanProcessor(exporter))
        self._closed = False

    def tracer(self, span_context=None):
        """Cria uma fachada leve que compartilha este provider e o contexto pai opcional."""
        if self._closed:
            raise RuntimeError("Zipkin tracing provider is closed")
        from .tracer import Tracer

        return Tracer(provider=self._provider, span_context=span_context)

    def force_flush(self, timeout_millis=30000):
        if self._closed:
            return False
        return self._provider.force_flush(timeout_millis=timeout_millis)

    def shutdown(self):
        if not self._closed:
            self._closed = True
            self._provider.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()


def _normalize_endpoint(endpoint):
    if not isinstance(endpoint, str) or not endpoint.strip():
        raise ValueError("Zipkin endpoint must be a non-empty HTTP(S) URL")
    endpoint = endpoint.strip().rstrip("/")
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Zipkin endpoint must be a non-empty HTTP(S) URL")
    if not parsed.path:
        endpoint += "/api/v2/spans"
    return endpoint
