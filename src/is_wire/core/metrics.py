from prometheus_client import Counter, Histogram

MESSAGES_PUBLISHED = Counter(
    "is_wire_messages_published_total",
    "Messages published through is-wire",
    ("exchange", "topic", "profile"),
)
PUBLISHED_BYTES = Counter(
    "is_wire_published_bytes_total",
    "Message body bytes published through is-wire",
    ("exchange", "topic", "profile"),
)
MESSAGES_RECEIVED = Counter(
    "is_wire_messages_received_total",
    "Messages received through is-wire",
    ("exchange", "topic", "profile"),
)
RECEIVED_BYTES = Counter(
    "is_wire_received_bytes_total",
    "Message body bytes received through is-wire",
    ("exchange", "topic", "profile"),
)
RECONNECTIONS = Counter(
    "is_wire_reconnections_total",
    "RabbitMQ reconnection attempts",
    ("exchange",),
)
STREAM_CALLBACK_ERRORS = Counter(
    "is_wire_stream_callback_errors_total",
    "Stream messages rejected after callback errors",
    ("exchange", "group"),
)
STREAM_FRAME_AGE = Histogram(
    "is_wire_stream_frame_age_seconds",
    "Age of a stream frame when delivered to its callback",
    ("exchange", "group"),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
)
STREAM_PROCESSING = Histogram(
    "is_wire_stream_processing_seconds",
    "Stream callback processing time",
    ("exchange", "group"),
)
