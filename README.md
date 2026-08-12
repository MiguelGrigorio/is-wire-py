# is-wire-sea

Middleware AMQP em Python para a arquitetura IS. A distribuição no PyPI se chama
`is-wire-sea`; o pacote Python estável continua sendo `is_wire`.

## Instalação

```shell
python -m pip install is-wire-sea
```

Há suporte para Python 3.10 a 3.14. Instale um exportador OTLP com:

```shell
python -m pip install 'is-wire-sea[tracing]'
```

## Broker

Implantações de produção devem fixar o RabbitMQ pela versão de patch e pelo digest da imagem.
A base de desenvolvimento é o RabbitMQ 4.3.1:

```shell
docker run --rm -p 5672:5672 \
  rabbitmq:4.3.1@sha256:6a46d2aef889d2a8cc28ac91b4a1ca0116a4a151de10cada219ee7685dc01c5b
```

RabbitMQ 3.7.6 é apenas um alvo de migração. Mova as implantações existentes da versão 3.7.6
para um cluster 4.3 novo usando uma migração blue-green; não reutilize diretamente o diretório
de dados antigo.

## Pub/sub

A API existente de entrega no máximo uma vez é preservada:

```python
from is_wire.core import Channel, Message, Subscription

with Channel("amqp://guest:guest@localhost:5672") as channel:
    subscription = Subscription(channel)
    subscription.subscribe("Camera.Config")

    channel.publish(Message(content=b"hello"), topic="Camera.Config")
    received = channel.consume(timeout=1.0)
```

Assinaturas anônimas são exclusivas e temporárias. Assinaturas nomeadas são duráveis,
compartilhadas entre réplicas e expiram após cinco minutos sem uso.

## Fluxos de imagens em tempo real

Use `StreamSubscription` quando quadros antigos puderem ser descartados. Réplicas com o mesmo
grupo compartilham uma fila, então cada quadro é processado por uma réplica em vez de ser
copiado para todas elas.

```python
from is_msgs.image_pb2 import Image
from is_wire.core import Channel, Message, StreamSubscription


def process(message):
    image = message.unpack(Image)
    # image.data já contém bytes JPEG, WebP ou PNG.


with Channel("amqp://guest:guest@localhost:5672") as channel:
    stream = StreamSubscription(channel, group="detector")
    stream.subscribe("Camera.*.Frame")
    stream.run(process)
```

Publique um quadro com semântica de fluxo:

```python
image = Image(data=encoded_jpeg_bytes)
channel.publish_stream(Message(content=image), topic="Camera.0.Frame")
```

O preset de fluxo usa confirmações manuais, prefetch 1, fila com tamanho um, descarte do item
mais antigo quando há overflow, TTL de dois segundos e mensagens não persistentes. Uma exceção
no callback rejeita o quadro sem recolocá-lo na fila. Use outro grupo somente quando outra
etapa de processamento realmente precisar de sua própria cópia.

`Image.data` deve conter bytes binários codificados da imagem. Não envie RGB/BGR bruto, a menos
que o orçamento de rede permita explicitamente; não codifique o corpo em base64 e não aplique
compressão gzip/zstd genérica a um payload JPEG, WebP ou PNG já comprimido.

Use um `Channel` separado para o tráfego de imagens e para o tráfego de RPC/controle, evitando
que quadros grandes atrasem mensagens pequenas de controle.

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

Há suporte para Protobuf 5 a 7. Os corpos binários existentes e as convenções de propriedades
AMQP continuam compatíveis com `is-wire` 1.2.1.

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

As filas de serviços RPC usam confirmações manuais e prefetch 16. Uma requisição só é
confirmada depois que sua resposta é publicada; por isso, os handlers devem ser idempotentes.

## OpenTelemetry

`Tracer`, `TracingInterceptor`, `Message.inject_tracing` e
`Message.extract_tracing` usam OpenTelemetry e propagação B3 com múltiplos cabeçalhos. IDs de
trace legados de 64 bits e IDs de 128 bits são aceitos.

```python
from is_wire.core import Message, Tracer

tracer = Tracer()
with tracer.span("publish") as span:
    message = Message(content=b"payload")
    message.inject_tracing(span)
```

### Zipkin

Instale o exportador nativo OpenTelemetry Zipkin JSON v2:

```shell
python -m pip install 'is-wire-sea[zipkin]'
```

Crie um provider compartilhado por processo. Ele agrupa as exportações em segundo plano, aplica
amostragem baseada no contexto pai e identifica cada instância de câmera separadamente no
Zipkin:

```python
from is_wire.core import Message, ZipkinTracing
from is_wire.rpc import ServiceProvider, TracingInterceptor

tracing = ZipkinTracing(
    service_name="camera-gateway",
    endpoint="http://zipkin:9411/api/v2/spans",
    service_instance_id="camera-5",
    sample_ratio=0.1,
    resource_attributes={"camera.driver": "hikvision"},
)
frame_tracer = tracing.tracer()

provider = ServiceProvider(rpc_channel)
provider.add_interceptor(TracingInterceptor(tracing=tracing))

with frame_tracer.span("camera.frame") as span:
    span.set_attribute("camera.id", "5")
    message = Message(content=image)
    message.inject_tracing(span)
    stream_channel.publish_stream(message, topic="CameraGateway.5.Frame")

# Libere os spans enfileirados durante o encerramento normal do processo.
tracing.shutdown()
```

Não associe corpos de imagem, credenciais, URLs de câmeras ou identificadores sem limite aos
spans. Use uma proporção de amostragem menor que um para vídeo contínuo e defina `1.0` apenas
em sessões curtas de diagnóstico. Consumidores continuam o trace do produtor com
`message.extract_tracing()`.

## Métricas

As métricas do Prometheus são registradas no registry padrão. `MetricsInterceptor.start_server()`
pode expô-las por HTTP. O perfil 2.0 exporta:

- `is_wire_messages_published_total` e `is_wire_published_bytes_total`;
- `is_wire_messages_received_total` e `is_wire_received_bytes_total`;
- `is_wire_stream_processing_seconds`, `is_wire_stream_frame_age_seconds` e
  `is_wire_stream_callback_errors_total`;
- `is_wire_reconnections_total`;
- `is_wire_rpc_duration_seconds` e `is_wire_rpc_requests_total`, particionados por status.

O conteúdo das imagens e os IDs de correlação nunca são usados como labels de métricas.

## TLS e ciclo de vida da conexão

`amqps://` habilita TLS com verificação de certificado e hostname. Opções SSL adicionais do
py-amqp podem ser passadas em `ssl_options`. Consumidores se reconectam com backoff exponencial;
publicações nunca são repetidas automaticamente, pois a entrega pode já ter ocorrido.

```python
channel = Channel(
    "amqps://user:password@rabbitmq.example:5671/vhost",
    heartbeat=30,
    ssl_options={"ca_certs": "/etc/ssl/certs/cluster-ca.pem"},
)
```

## Desenvolvimento

```shell
python -m pip install -e '.[dev,tracing]'
pytest
ruff check src tests
python -m build
twine check --strict dist/*
```

Regenere o schema wire interno de forma reproduzível com o compilador Protobuf 5.29 fixado:

```shell
python -m pip install -e '.[codegen]'
python scripts/generate_wire.py
```

Execute o perfil de publicador a 30 FPS / consumidor a 10 FPS contra um broker de teste com:

```shell
python scripts/validate_stream_profile.py --payload-size 1048576 --frames 300
```

Execute o mesmo comando em implantações Kubernetes separadas que compartilhem um `group` para
comparar as métricas de entrada/saída do RabbitMQ conforme réplicas são adicionadas.

Consulte [MIGRATION.md](MIGRATION.md), [COMPATIBILITY.md](COMPATIBILITY.md) e
[CHANGELOG.md](CHANGELOG.md) antes de atualizar uma implantação existente.

## Agradecimentos

A modernização, a revisão de compatibilidade, os testes e o empacotamento no PyPI da série 2.0
foram realizados com assistência do OpenAI Codex.
