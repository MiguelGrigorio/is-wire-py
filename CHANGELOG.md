# Registro de alterações

## 2.0.1

- Preserva a decisão B3 `sampled=false` ao propagar spans OpenTelemetry, impedindo que serviços
  filhos exportem traces órfãos que o serviço pai decidiu não amostrar.

## 2.0.0

- Adiciona um provider compartilhado e agrupado OpenTelemetry-to-Zipkin JSON v2, com amostragem
  baseada no contexto pai e atributos de recurso do serviço.
- Permite que interceptors de tracing RPC e facades leves de tracer compartilhem um provider.
- Remove o adaptador de exportação OpenCensus, o extra `legacy-tracing` e `AsyncTransport`.

## 1.3.1

- Atualiza o repositório do projeto e os metadados de contato do LabSEA.
- Registra a assistência do OpenAI Codex na modernização, revisão de compatibilidade, testes e
  empacotamento no PyPI.

## 1.3.0

- Publica a distribuição como `is-wire-sea`, preservando os imports `is_wire`.
- Oferece suporte a Python 3.10 a 3.14 e às versões atuais do Protobuf.
- Oferece suporte à semântica de filas do RabbitMQ 4.3.
- Adiciona assinaturas agrupadas do quadro mais recente e publicação transitória.
- Adiciona TLS, heartbeats, prefetch limitado, reconexão e encerramento limpo.
- Substitui os componentes internos OpenCensus por OpenTelemetry e preserva a propagação B3.
- Corrige confirmações RPC, métricas e estado de interceptors por chamada.

## 1.2.1

- Última versão upstream de `is-wire` usada como baseline de compatibilidade.
