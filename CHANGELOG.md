# Changelog

## 1.3.1

- Update the project repository and LabSEA contact metadata.
- Acknowledge OpenAI Codex assistance with modernization, compatibility review, testing, and
  PyPI packaging.

## 1.3.0

- Publish the distribution as `is-wire-sea` while preserving the `is_wire` imports.
- Support Python 3.10 through 3.14 and current Protobuf releases.
- Support RabbitMQ 4.3 queue semantics.
- Add grouped, latest-frame streaming subscriptions and transient stream publishing.
- Add TLS, heartbeats, bounded consumer prefetch, reconnecting consumers, and clean shutdown.
- Replace OpenCensus internals with OpenTelemetry and preserve B3 propagation.
- Correct RPC acknowledgements, metrics, and per-call interceptor state.

## 1.2.1

- Last upstream `is-wire` release used as the compatibility baseline.
