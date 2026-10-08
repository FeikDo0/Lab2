
import grpc
import argparse
import uuid
import time
import json
import os
import threading

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


clock = LamportClock()
log_lock = threading.Lock()

os.makedirs("logs", exist_ok=True)


def log_event(event, counter_id, lamport_time):
    data = {
        "event": event,
        "counter_id": counter_id,
        "lamport_time": lamport_time,
        "timestamp": time.time()
    }

    with log_lock:
        with open("logs/client.jsonl", "a") as file:
            file.write(json.dumps(data) + "\n")


def call_with_retry(stub, method, request):
    delays = [0.2, 0.4, 0.8]

    for attempt in range(4):
        try:
            send_time = clock.tick()
            request.lamport_time = send_time

            log_event(
                "SEND",
                request.counter_id,
                send_time
            )

            response = method(request, timeout=2.0)

            received_time = clock.update(
                response.lamport_time
            )

            log_event(
                "RECV",
                request.counter_id,
                received_time
            )

            return response

        except grpc.RpcError as error:
            if error.code() not in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE
            ):
                raise

            if attempt == 3:
                raise

            print(f"Retry {attempt + 1}...")
            time.sleep(delays[attempt])


def increment(stub, counter_id, delta, key=None):
    if key is None:
        key = str(uuid.uuid4())

    request = counter_pb2.IncrementRequest(
        counter_id=counter_id,
        delta=delta,
        idempotency_key=key
    )

    response = call_with_retry(
        stub,
        stub.Increment,
        request
    )

    print(
        f"OK value={response.new_value}, "
        f"duplicate={response.was_duplicate}"
    )


def get_value(stub, counter_id):
    request = counter_pb2.GetRequest(
        counter_id=counter_id
    )

    response = call_with_retry(
        stub,
        stub.Get,
        request
    )

    if response.found:
        print(f"value={response.value}")
    else:
        print("Counter not found")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--port",
        type=int,
        default=50051
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id")
    incr_parser.add_argument(
        "--by",
        type=int,
        default=1
    )
    incr_parser.add_argument("--key")

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id")

    args = parser.parse_args()

    with grpc.insecure_channel(
        f"localhost:{args.port}"
    ) as channel:

        stub = counter_pb2_grpc.CounterStub(channel)

        if args.command == "incr":
            increment(
                stub,
                args.counter_id,
                args.by,
                args.key
            )

        elif args.command == "get":
            get_value(stub, args.counter_id)


if __name__ == "__main__":
    main()
