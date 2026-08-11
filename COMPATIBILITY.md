# Compatibility policy

`is-wire-sea` follows semantic versioning for the public Python API and the existing IS wire
format. The distribution name changed, but applications continue to import `is_wire`.

Version 1.3 supports Python 3.10 through 3.14, py-amqp 5.x, Protobuf 5 through 7, and
RabbitMQ 4.3. RabbitMQ 3.7.6 and 3.13.7 are transition targets and are tested only to make
broker migration possible.

The `Subscription` and `Channel.consume` APIs retain their at-most-once legacy behavior.
`StreamSubscription` has intentionally lossy latest-frame semantics. RPC services use manual
acknowledgements and at-least-once request delivery, so handlers should be idempotent.

Binary Protobuf bodies and existing AMQP property conventions remain stable within the 1.x
series. Additive metadata is permitted; removing or reinterpreting an existing field requires
a major release.
