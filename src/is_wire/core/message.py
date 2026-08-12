from datetime import datetime

from google.protobuf import json_format as pb
from google.protobuf.struct_pb2 import Struct

from .subscription import Subscription
from .tracing.propagation import TextFormatPropagator
from .utils import assert_type, new_uuid, now
from .wire.content_type import ContentType
from .wire.status import Status


def _message_to_json(obj):
    try:
        return pb.MessageToJson(
            obj,
            indent=0,
            always_print_fields_with_no_presence=True,
        )
    except TypeError:  # Compatibilidade com Protobuf 3 durante a migração do broker.
        return pb.MessageToJson(
            obj,
            indent=0,
            including_default_value_fields=True,
        )


def _message_to_dict(obj):
    try:
        return pb.MessageToDict(
            obj,
            always_print_fields_with_no_presence=True,
        )
    except TypeError:  # Compatibilidade com Protobuf 3 durante a migração do broker.
        return pb.MessageToDict(
            obj,
            including_default_value_fields=True,
        )


class Message:

    def __init__(self, content=None, reply_to=None, content_type=None):
        """Cria uma nova mensagem.

        Args:
            content (str or object): define o corpo da mensagem. Se um objeto
                for fornecido, ele será empacotado usando content_type.
            content_type (ContentType): descreve como o conteúdo será serializado.
            reply_to (str or Subscription): indica para onde a resposta deve ser enviada.
        """
        self._topic = None
        self._body = b''
        self._reply_to = None
        self._subscription_id = None
        self._correlation_id = None
        self._content_type = None
        self._created_at = now()
        self._metadata = {}
        self._timeout = None
        self._status = None
        self._delivery_channel = None
        self._delivery_tag = None
        self._acknowledgeable = False
        self._settled = False

        if reply_to is not None:
            self.reply_to = reply_to

        if content_type is not None:
            self.content_type = content_type

        if content is not None:
            if isinstance(content, bytes):
                self.body = content
            else:
                self.pack(content)

    def __str__(self):
        """Converte a mensagem em uma string detalhada de suas propriedades."""
        created_at = datetime.fromtimestamp(self.created_at)
        pretty = "{\n"
        pretty += "  topic = '{}'\n".format(self.topic or "")
        pretty += f"  created_at = {created_at}\n"
        pretty += f"  correlation_id = {self.correlation_id}\n"
        pretty += "  reply_to = '{}'\n".format(self.reply_to or "")
        pretty += "  subscription_id = '{}'\n".format(self.subscription_id
                                                      or "")
        pretty += f"  timeout = {self.timeout}\n"
        pretty += f"  status = {self.status}\n"
        pretty += f"  metadata = {self.metadata}\n"
        pretty += f"  content_type = {self.content_type}\n"
        pretty += f"  body[{len(self.body)}] = {repr(self.body)} \n"
        pretty += "}"
        return pretty

    def short_string(self):
        """Converte a mensagem em uma string simplificada; campos vazios não são exibidos."""
        created_at = datetime.fromtimestamp(self.created_at)
        pretty = "{"
        pretty += f"topic='{self.topic}'"
        pretty += f" created_at={created_at}"
        if self.has_correlation_id():
            pretty += f" correlation_id={self.correlation_id}"
        if self.has_reply_to():
            pretty += f" reply_to='{self.reply_to}'"
        if self.has_subscription_id():
            pretty += f" subscription_id='{self.subscription_id}'"
        if self.has_timeout():
            pretty += f" timeout={self.timeout}"
        if self.has_status():
            pretty += f" status={self.status}"
        if self.has_metadata():
            pretty += f" metadata={self.metadata}"
        if self.has_content_type():
            pretty += f" content_type={self.content_type}"
        pretty += f" body[{len(self.body)}]={repr(self.body)}"
        pretty += "}"
        return pretty

    def __eq__(self, other):
        """Retorna True se as mensagens forem iguais e False caso contrário."""
        if not isinstance(other, Message):
            return NotImplemented
        ignored = {
            "_delivery_channel",
            "_delivery_tag",
            "_acknowledgeable",
            "_settled",
        }
        mine = {key: value for key, value in self.__dict__.items() if key not in ignored}
        theirs = {key: value for key, value in other.__dict__.items() if key not in ignored}
        return mine == theirs

    def create_reply(self):
        reply = Message()
        if self.has_reply_to():
            reply.topic = self.reply_to
        if self.has_correlation_id():
            reply.correlation_id = self.correlation_id
        if self.has_content_type():
            reply.content_type = self.content_type
        return reply

    # tópico

    @property
    def topic(self):
        """str: Tópico em que a mensagem foi ou será publicada."""
        return self._topic

    @topic.setter
    def topic(self, topic):
        assert_type(topic, str, "topic")
        self._topic = topic

    def has_topic(self):
        """Retorna True se a propriedade topic estiver definida."""
        return bool(self._topic)

    # reply_to

    @property
    def reply_to(self):
        """str: Tópico em que a resposta deve ser publicada.

        Um objeto Subscription pode ser passado para definir esse valor
        automaticamente. O campo correlation_id é definido automaticamente se vazio.
        """
        return self._reply_to

    @reply_to.setter
    def reply_to(self, value):
        assert_type(value, (str, Subscription), "reply_to")

        if self.correlation_id is None:
            self.correlation_id = new_uuid()

        if isinstance(value, Subscription):
            self._reply_to = value.name
            self.subscription_id = value.id

        elif isinstance(value, str):
            self._reply_to = value

    def has_reply_to(self):
        """Retorna True se a propriedade reply_to estiver definida."""
        return bool(self._reply_to)

    # subscription_id

    @property
    def subscription_id(self):
        """str: ID da assinatura à qual esta mensagem pertence."""
        return self._subscription_id

    @subscription_id.setter
    def subscription_id(self, value):
        assert_type(value, str, "subscription_id")
        self._subscription_id = value

    def has_subscription_id(self):
        """Retorna True se a propriedade subscription_id estiver definida."""
        return bool(self._subscription_id)

    # correlation_id

    @property
    def correlation_id(self):
        """int: ID exclusivo usado para correlacionar mensagens de resposta."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value):
        assert_type(value, int, "correlation_id")
        self._correlation_id = value

    def has_correlation_id(self):
        """Retorna True se a propriedade correlation_id estiver definida."""
        return self._correlation_id is not None

    # corpo

    @property
    def body(self):
        """bytes: Conteúdo bruto da mensagem."""
        return self._body

    @body.setter
    def body(self, value):
        assert_type(value, bytes, "body")
        self._body = value

    def has_body(self):
        """Retorna True se a propriedade body estiver definida."""
        return bool(self._body)

    # content_type

    @property
    def content_type(self):
        """ContentType: indica como o conteúdo/corpo da mensagem foi serializado."""
        return self._content_type

    @content_type.setter
    def content_type(self, value):
        assert_type(value, ContentType, "content_type")
        self._content_type = value

    def has_content_type(self):
        """Retorna True se a propriedade content_type estiver definida."""
        return self._content_type is not None

    # created_at

    @property
    def created_at(self):
        """float: Segundos desde a época indicando quando a mensagem foi criada."""
        return self._created_at

    @created_at.setter
    def created_at(self, timestamp):
        assert_type(timestamp, (int, float), "created_at")
        self._created_at = timestamp

    def has_created_at(self):
        """Retorna True se a propriedade created_at estiver definida."""
        return self._created_at is not None

    # metadados

    @property
    def metadata(self):
        """dict: Pares chave-valor que podem representar informações extras da mensagem."""
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        assert_type(value, dict, "metadata")
        self._metadata = value

    def has_metadata(self):
        """Retorna True se a propriedade metadata estiver definida."""
        return len(self._metadata) != 0

    # timeout

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, seconds):
        assert_type(seconds, (float, int), "timeout")
        if seconds < 0:
            raise ValueError("timeout must be greater than or equal to zero")
        self._timeout = seconds

    def has_timeout(self):
        """Retorna True se a propriedade timeout estiver definida."""
        return self._timeout is not None

    def deadline_exceeded(self):
        if not self.has_timeout():
            return False
        return now() > self.created_at + self.timeout

    # status

    @property
    def status(self):
        """Status que representa o sucesso ou a falha de uma RPC."""
        return self._status

    @status.setter
    def status(self, value):
        assert_type(value, Status, "status")
        self._status = value

    def has_status(self):
        """Retorna True se a propriedade status estiver definida."""
        return self._status is not None

    # tracing

    def extract_tracing(self):
        return TextFormatPropagator.from_carrier(self.metadata)

    def inject_tracing(self, span):
        self.metadata = TextFormatPropagator.to_carrier(span, self.metadata)

    # confirmações de entrega

    def _set_delivery(self, channel, delivery_tag, acknowledgeable):
        self._delivery_channel = channel
        self._delivery_tag = delivery_tag
        self._acknowledgeable = acknowledgeable

    @property
    def acknowledgeable(self):
        return self._acknowledgeable and not self._settled

    def ack(self):
        """Confirma esta entrega quando a confirmação manual está habilitada."""
        if not self.acknowledgeable:
            return False
        self._delivery_channel.basic_ack(self._delivery_tag)
        self._settled = True
        return True

    def reject(self, requeue=False):
        """Rejeita esta entrega, opcionalmente pedindo ao RabbitMQ que a reencaminhe à fila."""
        if not self.acknowledgeable:
            return False
        self._delivery_channel.basic_reject(self._delivery_tag, requeue=requeue)
        self._settled = True
        return True

    # empacotamento / desempacotamento

    def pack(self, obj):
        """Serializa o objeto usando o content_type especificado da mensagem.

        Se a mensagem não tiver content_type, o formato Protobuf será usado.
        Args:
            obj (object): objeto Protobuf a ser serializado.
        """

        isDict = isinstance(obj, dict)

        if isDict:
            obj = pb.ParseDict(obj, Struct())

        if not self.has_content_type():
            # Por padrão, dicionários são serializados como JSON.
            # Objetos Protobuf são serializados em formato binário.
            if isDict:
                self.content_type = ContentType.JSON
            else:
                self.content_type = ContentType.PROTOBUF

        if self.content_type == ContentType.PROTOBUF:
            # SerializeToString retorna str no Python 2 e bytes no Python 3.
            self.body = obj.SerializeToString()
        elif self.content_type == ContentType.JSON:
            # MessageToJson retorna str no Python 2 e no Python 3.
            packed = _message_to_json(obj)
            if not isinstance(packed, bytes):
                self.body = packed.encode('latin')
            else:
                self.body = packed
        else:
            raise NotImplementedError(
                f"Serialization to '{self.content_type.name}' type not implemented")

    def unpack(self, schema=dict):
        """Desserializa o conteúdo usando o schema fornecido.

        Se a mensagem não tiver content_type, o formato Protobuf será usado.
        Args:
            schema (type): tipo do objeto Protobuf a ser desserializado.
        Returns:
            schema: instância desserializada do tipo schema.
        """

        isDict = schema is dict
        if isDict:
            schema = Struct

        obj = schema()
        if not self.has_content_type():
            self.content_type = ContentType.PROTOBUF

        if self.content_type == ContentType.PROTOBUF:
            obj.ParseFromString(self.body)
        elif self.content_type == ContentType.JSON:
            obj = pb.Parse(self.body, obj)
        else:
            raise NotImplementedError(
                f"Deserialization from '{self.content_type.name}' type not implemented")

        if isDict:
            obj = _message_to_dict(obj)

        return obj
