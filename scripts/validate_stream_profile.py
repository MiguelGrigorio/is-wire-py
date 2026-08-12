"""Exercita o perfil de fluxo do quadro mais recente contra um broker RabbitMQ real."""

import argparse
import json
import statistics
import time
from threading import Event, Thread

from is_wire.core import Channel, Message, StreamSubscription, now


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * fraction)
    return ordered[index]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="amqp://guest:guest@localhost:5672")
    parser.add_argument("--topic", default="Camera.0.Frame.Benchmark")
    parser.add_argument("--group", default="stream-benchmark")
    parser.add_argument("--frames", type=int, default=300)
    parser.add_argument("--payload-size", type=int, default=1024 * 1024)
    parser.add_argument("--publish-fps", type=float, default=30.0)
    parser.add_argument("--process-fps", type=float, default=10.0)
    return parser.parse_args()


def main():
    args = parse_args()
    if args.frames <= 0 or args.payload_size < 8:
        raise ValueError("frames must be positive and payload-size must be at least eight")
    if args.publish_fps <= 0 or args.process_fps <= 0:
        raise ValueError("publish-fps and process-fps must be positive")

    published = Event()
    received_sequences = []
    frame_ages = []
    padding = bytes(args.payload_size - 8)
    started = time.monotonic()

    with Channel(args.uri) as consumer_channel:
        stream = StreamSubscription(consumer_channel, group=args.group)
        stream.subscribe(args.topic)

        def produce():
            period = 1.0 / args.publish_fps
            with Channel(args.uri) as producer_channel:
                for sequence in range(args.frames):
                    body = sequence.to_bytes(8, "big") + padding
                    producer_channel.publish_stream(Message(body), topic=args.topic)
                    time.sleep(period)
            published.set()

        def watchdog():
            expected = args.frames / args.publish_fps
            if not published.wait(expected + 10.0):
                stream.stop()
                return
            time.sleep(3.0)
            stream.stop()

        def process(message):
            sequence = int.from_bytes(message.body[:8], "big")
            received_sequences.append(sequence)
            frame_ages.append(max(0.0, now() - message.created_at))
            time.sleep(1.0 / args.process_fps)
            if published.is_set() and sequence == args.frames - 1:
                stream.stop()

        producer = Thread(target=produce, daemon=True)
        producer.start()
        Thread(target=watchdog, daemon=True).start()
        stream.run(process)
        producer.join()

    elapsed = time.monotonic() - started
    processed = len(received_sequences)
    result = {
        "published_frames": args.frames,
        "processed_frames": processed,
        "superseded_frames": args.frames - processed,
        "last_sequence": received_sequences[-1] if received_sequences else None,
        "payload_bytes": args.payload_size,
        "publisher_ingress_bytes": args.frames * args.payload_size,
        "consumer_egress_bytes": processed * args.payload_size,
        "elapsed_seconds": elapsed,
        "effective_processing_fps": processed / elapsed,
        "frame_age_seconds": {
            "mean": statistics.fmean(frame_ages) if frame_ages else None,
            "p50": percentile(frame_ages, 0.50),
            "p95": percentile(frame_ages, 0.95),
            "p99": percentile(frame_ages, 0.99),
        },
    }
    print(json.dumps(result, indent=2))

    if result["last_sequence"] != args.frames - 1:
        raise RuntimeError("the latest published frame was not observed")
    if any(age > 2.1 for age in frame_ages):
        raise RuntimeError("a delivered frame exceeded the two-second stream TTL")


if __name__ == "__main__":
    main()
