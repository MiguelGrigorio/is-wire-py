# Migrating to is-wire-sea 1.3

## Python package

Replace `is-wire` with `is-wire-sea` in dependency manifests. Do not change application
imports: the package is still imported as `is_wire`. Upgrade the runtime to Python 3.10 or
newer.

Applications using OpenCensus should temporarily install `is-wire-sea[legacy-tracing]` and
migrate their exporter to OpenTelemetry/OTLP. New applications should use
`is-wire-sea[tracing]`.

## Queues

Anonymous subscriptions now declare exclusive transient queues. Named subscriptions and RPC
services declare durable shared queues with a five-minute unused-queue expiration. If a named
queue with the old non-durable properties still exists, stop its consumers and delete that
queue before starting 1.3; RabbitMQ rejects redeclaration with different properties.

Keep `Subscription` for configuration/events where every subscriber needs a copy. Change
real-time image consumers to `StreamSubscription` and assign one stable group per processing
stage. All replicas of a stage must use exactly the same group.

## Broker

Validate 1.3 against the existing RabbitMQ deployment first. A RabbitMQ 3.7.6 data directory
cannot be upgraded directly to 4.3. Create a new pinned 4.3 cluster, reproduce users/vhosts,
permissions, policies and exchanges, then move applications incrementally. Do not enable
`transient_nonexcl_queues` as a permanent compatibility setting.

## Operational checks

- Use separate channels/connections for high-volume image traffic and RPC.
- Confirm image publishers send JPEG, WebP, or PNG bytes rather than raw arrays or base64.
- Verify each replicated stage shares one stream group.
- Monitor published/received bytes, frame age, callback errors, RPC status and reconnections.
- Treat RPC handlers as idempotent because unacknowledged requests can be redelivered.
