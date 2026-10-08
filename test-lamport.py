
import grpc
import threading
import uuid
import json
import os

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


os.makedirs("logs", exist_ok=True)

log_lock = threading.Lock()
events = []


def record(client, event, operation, lamport):
    with log_lock:
        events.append({
            "client": client,
            "event": event,
            "operation": operation,
            "lamport": lamport
        })


def run_client(name, counter_id, increments):
    clock = LamportClock()

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = counter_pb2_grpc.CounterStub(channel)

        for _ in range(increments):
            send_time = clock.tick()
            record(name, "SEND", "Increment", send_time)

            response = stub.Increment(
                counter_pb2.IncrementRequest(
                    counter_id=counter_id,
                    delta=1,
                    idempotency_key=str(uuid.uuid4()),
                    lamport_time=send_time
                ),
                timeout=3.0
            )

            receive_time = clock.update(
                response.lamport_time
            )
            record(name, "RECV", "IncrementReply", receive_time)


def run_reader():
    clock = LamportClock()

    with grpc.insecure_channel("localhost:50051") as channel:
        stub = counter_pb2_grpc.CounterStub(channel)

        for _ in range(2):
            send_time = clock.tick()
            record("client-3", "SEND", "Get", send_time)

            response = stub.Get(
                counter_pb2.GetRequest(
                    counter_id="lamport_x",
                    lamport_time=send_time
                ),
                timeout=3.0
            )

            receive_time = clock.update(
                response.lamport_time
            )
            record("client-3", "RECV", "GetReply", receive_time)


threads = [
    threading.Thread(
        target=run_client,
        args=("client-1", "lamport_x", 2)
    ),
    threading.Thread(
        target=run_client,
        args=("client-2", "lamport_y", 2)
    ),
    threading.Thread(target=run_reader)
]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

with open("logs/lamport-trace.jsonl", "w") as file:
    for event in events:
        file.write(json.dumps(event) + "\n")

for event in events:
    print(event)

print("LAMPORT TRACE COMPLETED")
