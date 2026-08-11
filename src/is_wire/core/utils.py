import uuid
from platform import uname
from time import time


def new_uuid():
    return uuid.uuid4().int >> 64


def consumer_id():
    return f'{uname()[1]}/{new_uuid():X}'


def now():
    return time()


def assert_type(instance, types, name):
    if isinstance(types, list):
        types = tuple(types)

    if not isinstance(instance, types):
        input_type = type(instance).__name__
        if isinstance(types, tuple):
            types = " or ".join([t.__name__ for t in types])
            error = f"Object {name} must be of types {types}, received type {input_type}"
        else:
            error = f"Object {name} must be of type {types.__name__}, received type {input_type}"
        raise TypeError(error)
