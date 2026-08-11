"""Regenerate the checked-in wire Protocol Buffers module."""

from pathlib import Path

from grpc_tools import protoc


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    wire_dir = root / "src" / "is_wire" / "core" / "wire"
    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"--proto_path={wire_dir}",
            f"--python_out={wire_dir}",
            str(wire_dir / "wire.proto"),
        ]
    )
    if result == 0:
        generated = wire_dir / "wire_pb2.py"
        # protoc escapes double quotes inside its single-quoted descriptor
        # literal. Normalizing those redundant escapes keeps the checked-in
        # output stable across formatting tools without changing its bytes.
        generated.write_text(generated.read_text().replace(r'\"', '"'))
    return result


if __name__ == "__main__":
    raise SystemExit(main())
