from google.protobuf import json_format

from ..message import Message
from . import wire_pb2
from .content_type import content_type_from_wire, content_type_to_wire
from .status import Status, StatusCode


def _message_to_json(message):
    try:
        return json_format.MessageToJson(
            message,
            indent=0,
            always_print_fields_with_no_presence=True,
        )
    except TypeError:  # Compatibilidade com Protobuf 3 durante a migração do broker.
        return json_format.MessageToJson(
            message,
            indent=0,
            including_default_value_fields=True,
        )


class WireV1:

    @staticmethod
    def from_amqp_message(amqp_message, acknowledgeable=False):
        message = Message()

        if not isinstance(amqp_message.body, bytes):
            message.body = amqp_message.body.encode('latin')
        else:
            message.body = amqp_message.body

        delivery_info = amqp_message.delivery_info
        message.topic = delivery_info["routing_key"]
        message.subscription_id = delivery_info["consumer_tag"]

        properties = amqp_message.properties
        if "content_type" in properties:
            message.content_type = content_type_from_wire(
                properties["content_type"])

        if "correlation_id" in properties:
            message.correlation_id = int(properties["correlation_id"], 16)

        if "reply_to" in properties:
            message.reply_to = properties["reply_to"]

        if "expiration" in properties:
            message.timeout = int(properties["expiration"]) / 1000.0

        if "timestamp" in properties:
            message.created_at = properties["timestamp"] / 1000.0

        if "application_headers" in properties:
            headers = dict(properties["application_headers"])
            if "rpc-status" in headers:
                status = json_format.Parse(
                    headers.pop("rpc-status"),
                    wire_pb2.Status())
                message.status = Status(
                    code=StatusCode(status.code),
                    why=status.why,
                )
            message.metadata = headers

        message._set_delivery(
            amqp_message.channel,
            delivery_info.get("delivery_tag"),
            acknowledgeable,
        )

        return message

    @staticmethod
    def to_amqp_properties(message, *, expiration=None, delivery_mode=None):
        properties = {}
        properties["timestamp"] = int(message.created_at * 1000)

        if message.has_content_type():
            properties["content_type"] = content_type_to_wire(
                message.content_type)

        if message.has_correlation_id():
            properties["correlation_id"] = f"{message.correlation_id:X}"

        if message.has_reply_to():
            properties["reply_to"] = message.reply_to

        if message.has_timeout():
            properties["expiration"] = str(int(message.timeout * 1000))

        if expiration is not None:
            properties["expiration"] = str(int(expiration * 1000))

        if delivery_mode is not None:
            properties["delivery_mode"] = delivery_mode

        properties["application_headers"] = dict(message.metadata)

        if message.has_status():
            status = wire_pb2.Status(
                code=message.status.code.value,
                why=message.status.why,
            )
            properties["application_headers"][
                "rpc-status"] = _message_to_json(status)

        return properties
