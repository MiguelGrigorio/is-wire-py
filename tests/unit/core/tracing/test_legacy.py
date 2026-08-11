import pytest

from is_wire.core import Tracer

pytest.importorskip("opentelemetry.sdk.trace")
print_exporter = pytest.importorskip("opencensus.trace.print_exporter")


def test_opencensus_exporter_adapter_is_available_with_legacy_extra(capsys):
    with pytest.warns(DeprecationWarning, match="OpenCensus exporters are deprecated"):
        tracer = Tracer(exporter=print_exporter.PrintExporter())

    with tracer.span("legacy.span"):
        pass
    tracer.shutdown()

    assert "legacy.span" in capsys.readouterr().out
