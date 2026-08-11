from dataclasses import dataclass


@dataclass(frozen=True)
class TracingContext:
    trace_id: str
    span_id: str
    sampled: bool = True


class TextFormatPropagator:
    """Compatibility B3 multi-header propagator.

    B3 permits 64-bit and 128-bit trace identifiers. New traces use 128-bit
    identifiers while contexts extracted from older services retain their
    original representation.
    """

    trace_prefix = "x-b3"
    trace_id_key = f"{trace_prefix}-traceid"
    span_id_key = f"{trace_prefix}-spanid"
    parent_span_id_key = f"{trace_prefix}-parentspanid"
    sampled_key = f"{trace_prefix}-sampled"
    flags_key = f"{trace_prefix}-flags"

    @classmethod
    def from_carrier(cls, carrier):
        if cls.trace_id_key not in carrier or cls.span_id_key not in carrier:
            return None
        return TracingContext(
            trace_id=str(carrier[cls.trace_id_key]).lower(),
            span_id=str(carrier[cls.span_id_key]).lower(),
            sampled=str(carrier.get(cls.sampled_key, "1")) != "0",
        )

    @classmethod
    def to_carrier(cls, span_or_context, carrier):
        trace_id, span_id, sampled = cls._ids(span_or_context)
        output = dict(carrier)
        output[cls.trace_id_key] = trace_id
        output[cls.span_id_key] = span_id
        output[cls.sampled_key] = "1" if sampled else "0"
        output[cls.parent_span_id_key] = "0" * 16
        output[cls.flags_key] = "0"
        return output

    @staticmethod
    def new_span_context(trace_id, span_id):
        return TracingContext(str(trace_id), str(span_id))

    @staticmethod
    def _ids(span_or_context):
        if isinstance(span_or_context, TracingContext):
            return (
                span_or_context.trace_id,
                span_or_context.span_id,
                span_or_context.sampled,
            )

        if hasattr(span_or_context, "context_tracer") and hasattr(span_or_context, "span_id"):
            return (
                str(span_or_context.context_tracer.trace_id),
                str(span_or_context.span_id),
                True,
            )

        context = span_or_context.get_span_context()
        trace_id = f"{context.trace_id:032x}"
        span_id = f"{context.span_id:016x}"
        sampled = bool(int(context.trace_flags) & 1)
        return trace_id, span_id, sampled
