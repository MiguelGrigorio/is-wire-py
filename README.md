# is-wire-sea

Python AMQP middleware for the IS architecture. The PyPI distribution is named
`is-wire-sea`; the stable Python package remains `is_wire`.

## Installation

```shell
python -m pip install is-wire-sea
```

Python 3.10 through 3.14 are supported. Install an OTLP exporter with:

```shell
python -m pip install 'is-wire-sea[tracing]'
```

## Broker

Production deployments must pin RabbitMQ by patch version and image digest. The development
baseline is RabbitMQ 4.3.1:

```shell
docker run --rm -p 5672:5672 \
  rabbitmq:4.3.1@sha256:6a46d2aef889d2a8cc28ac91b4a1ca0116a4a151de10cada219ee7685dc01c5b
```

RabbitMQ 3.7.6 is a migration-only target. Move existing 3.7.6 deployments to a fresh 4.3
cluster using a blue-green migration; do not reuse its data directory directly.

## Pub/sub

The existing at-most-once API is preserved:

```python
from is_wire.core import Channel, Message, Subscription

with Channel("amqp://guest:guest@localhost:5672") as channel:
    subscription = Subscription(channel)
    subscription.subscribe("Camera.Config")

    channel.publish(Message(content=b"hello"), topic="Camera.Config")
    received = channel.consume(timeout=1.0)
```

Anonymous subscriptions are exclusive and temporary. Named subscriptions are durable,
shared between replicas, and expire after five minutes without use.

## Real-time image streams

Use `StreamSubscription` when old frames may be discarded. Replicas with the same group
share one queue, so each frame is processed by one replica instead of being copied to all of
them.

```python
from is_msgs.image_pb2 import Image
from is_wire.core import Channel, Message, StreamSubscription


def process(message):
    image = message.unpack(Image)
    # image.data already contains JPEG, WebP, or PNG bytes.


with Channel("amqp://guest:guest@localhost:5672") as channel:
    stream = StreamSubscription(channel, group="detector")
    stream.subscribe("Camera.*.Frame")
    stream.run(process)
```

Publish a frame with stream semantics:

```python
image = Image(data=encoded_jpeg_bytes)
channel.publish_stream(Message(content=image), topic="Camera.0.Frame")
```

The stream preset uses manual acknowledgements, prefetch 1, a queue length of one, drop-head
overflow, a two-second TTL, and non-persistent messages. A callback exception rejects the
frame without requeue. Use a different group only when another processing stage genuinely
needs its own copy.

`Image.data` must contain encoded binary image bytes. Do not send raw RGB/BGR unless the
network budget explicitly allows it, do not base64 encode the body, and do not apply generic
gzip/zstd compression to an already compressed JPEG, WebP, or PNG payload.

Use a separate `Channel` for image traffic and RPC/control traffic to avoid large frames
delaying small control messages.

## Protobuf

```python
from google.protobuf.struct_pb2 import Struct
from is_wire.core import ContentType, Message

value = Struct()
value["camera"] = "front"

binary = Message(content=value)
assert binary.content_type == ContentType.PROTOBUF
assert binary.unpack(Struct) == value

json_message = Message(content_type=ContentType.JSON)
json_message.pack(value)
```

Protobuf 5 through 7 are supported. The existing binary bodies and AMQP property conventions
remain compatible with `is-wire` 1.2.1.

## RPC

```python
from google.protobuf.struct_pb2 import Struct
from is_wire.core import Channel
from is_wire.rpc import ServiceProvider


def echo(request, context):
    return request


channel = Channel("amqp://guest:guest@localhost:5672")
provider = ServiceProvider(channel)
provider.delegate("Echo", echo, Struct, Struct)
provider.run()
```

RPC service queues use manual acknowledgements and prefetch 16. A request is acknowledged
only after its reply is published, so handlers should be idempotent.

## OpenTelemetry

`Tracer`, `TracingInterceptor`, `Message.inject_tracing`, and
`Message.extract_tracing` use OpenTelemetry and B3 multi-header propagation. Both 64-bit
legacy and 128-bit trace IDs are accepted.

```python
from is_wire.core import Message, Tracer

tracer = Tracer()
with tracer.span("publish") as span:
    message = Message(content=b"payload")
    message.inject_tracing(span)
```

OpenCensus exporters are available temporarily through `is-wire-sea[legacy-tracing]` and emit
a deprecation warning.

## Metrics

Prometheus metrics are registered in the default registry. `MetricsInterceptor.start_server()`
can expose them over HTTP. The 1.3 profile exports:

- `is_wire_messages_published_total` and `is_wire_published_bytes_total`;
- `is_wire_messages_received_total` and `is_wire_received_bytes_total`;
- `is_wire_stream_processing_seconds`, `is_wire_stream_frame_age_seconds`, and
  `is_wire_stream_callback_errors_total`;
- `is_wire_reconnections_total`;
- `is_wire_rpc_duration_seconds` and `is_wire_rpc_requests_total`, partitioned by status.

Image content and correlation IDs are never used as metric labels.

## TLS and connection lifecycle

`amqps://` enables TLS with certificate and hostname verification. Additional py-amqp SSL
options can be passed with `ssl_options`. Consumers reconnect with exponential backoff;
publishes are never automatically retried because delivery may already have occurred.

```python
channel = Channel(
    "amqps://user:password@rabbitmq.example:5671/vhost",
    heartbeat=30,
    ssl_options={"ca_certs": "/etc/ssl/certs/cluster-ca.pem"},
)
```

## Development

```shell
python -m pip install -e '.[dev,tracing]'
pytest
ruff check src tests
python -m build
twine check --strict dist/*
```

Regenerate the internal wire schema reproducibly with the pinned Protobuf 5.29 compiler:

```shell
python -m pip install -e '.[codegen]'
python scripts/generate_wire.py
```

Exercise the 30 FPS publisher / 10 FPS consumer profile against a test broker with:

```shell
python scripts/validate_stream_profile.py --payload-size 1048576 --frames 300
```

Run the same command from separate Kubernetes deployments sharing a `group` to compare
RabbitMQ ingress/egress metrics as replicas are added.

See [MIGRATION.md](MIGRATION.md), [COMPATIBILITY.md](COMPATIBILITY.md), and
[CHANGELOG.md](CHANGELOG.md) before upgrading an existing deployment.

## Acknowledgements

The modernization, compatibility review, testing, and PyPI packaging of the 1.3 series were
completed with assistance from OpenAI Codex.
