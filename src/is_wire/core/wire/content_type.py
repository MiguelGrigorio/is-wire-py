from enum import Enum

from ..utils import assert_type
from . import wire_pb2


class ContentType(Enum):
    PROTOBUF = wire_pb2.ContentType.Value("PROTOBUF")
    JSON = wire_pb2.ContentType.Value("JSON")


def content_type_to_wire(content_type):
    """Converte um objeto ContentType para a representação textual no wire.
    Args:
        content_type (ContentType): valor do enum
    Returns:
        str: representação textual no wire
    """
    assert_type(content_type, ContentType, "content_type")
    if content_type == ContentType.PROTOBUF:
        return 'application/x-protobuf'

    if content_type == ContentType.JSON:
        return 'application/json'

    raise NotImplementedError(
        f"ContentType '{content_type.name}' wire serialization not implemented")


def content_type_from_wire(string):
    """Converte a representação textual do ContentType no wire para o enum.
    Args:
        string (str): representação textual no wire
    Returns:
        ContentType: valor do enum
    """
    assert_type(string, str, "string")
    if string == 'application/x-protobuf':
        return ContentType.PROTOBUF

    if string == 'application/json':
        return ContentType.JSON

    raise RuntimeError(f"Bad content_type {string}")
