
import time
import grpc
import threading
import argparse
import json
import os

from concurrent import futures

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):

    def __init__(self):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self.clock = LamportClock()

        os.makedirs("logs", exist_ok=True)

    def log_event(self, event, counter_id, lamport_time):
        data = {
            "event": event,
            "counter_id": counter_id,
            "lamport_time": lamport_time,
            "timestamp": time.time()
        }

        with open("logs/server.jsonl", "a") as file:
            file.write(json.dumps(data) + "\n")

    def Increment(self, request, context):
        with self._lock:

            received_time = self.clock.update(
                request.lamport_time
            )

            self.log_event(
                "RECV",
                request.counter_id,
                received_time
            )

            key = request.idempotency_key

            if key in self._seen:
                send_time = self.clock.tick()

                self.log_event(
                    "SEND",
                    request.counter_id,
                    send_time
                )

                return counter_pb2.IncrementReply(
                    new_value=self._seen[key],
                    was_duplicate=True,
                    lamport_time=send_time
                )

            if request.counter_id == "timeout_test":
                time.sleep(0.3)

            counter_id = request.counter_id
            delta = request.delta

            current_value = self._values.get(counter_id, 0)
            new_value = current_value + delta

            self._values[counter_id] = new_value
            self._seen[key] = new_value

            apply_time = self.clock.tick()

            self.log_event(
                "APPLY",
                counter_id,
                apply_time
            )

            send_time = self.clock.tick()

            self.log_event(
                "SEND",
                counter_id,
                send_time
            )

            return counter_pb2.IncrementReply(
                new_value=new_value,
                was_duplicate=False,
                lamport_time=send_time
            )

    def Get(self, request, context):
        with self._lock:

            received_time = self.clock.update(
                request.lamport_time
            )

            self.log_event(
                "RECV",
                request.counter_id,
                received_time
            )

            counter_id = request.counter_id

            value = self._values.get(counter_id, 0)
            found = counter_id in self._values

            send_time = self.clock.tick()

            self.log_event(
                "SEND",
                counter_id,
                send_time
            )

            return counter_pb2.GetReply(
                value=value,
                found=found,
                lamport_time=send_time
            )

    def Restore(self, request, context):
        with self._lock:

            received_time = self.clock.update(
                request.lamport_time
            )

            self.log_event(
                "RECV",
                request.counter_id,
                received_time
            )

            self._values[request.counter_id] = request.value

            apply_time = self.clock.tick()

            self.log_event(
                "RESTORE",
                request.counter_id,
                apply_time
            )

            send_time = self.clock.tick()

            self.log_event(
                "SEND",
                request.counter_id,
                send_time
            )

            return counter_pb2.RestoreReply(
                success=True,
                lamport_time=send_time
            )


def serve(port):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8)
    )

    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(),
        server
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()

    print(f"Server started on port {port}")

    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)

    args = parser.parse_args()

    serve(args.port)
