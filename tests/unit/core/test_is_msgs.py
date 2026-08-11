import pytest

from is_wire.core import Message

image_pb2 = pytest.importorskip("is_msgs.image_pb2")


def test_current_is_msgs_image_round_trip_preserves_encoded_bytes():
    image = image_pb2.Image(data=b"\xff\xd8encoded-jpeg\xff\xd9")

    decoded = Message(image).unpack(image_pb2.Image)

    assert decoded == image
    assert decoded.data == image.data
