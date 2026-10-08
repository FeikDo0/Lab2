
import sys
import os
import time
import uuid
import math
import statistics
from concurrent.futures import ThreadPoolExecutor

import grpc

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(__file__))
)

import counter_pb2
import counter_pb2_grpc
from replication import ReplicationManager


REQUESTS = 2000
PORTS = [50051, 50052, 50053]


def single_increment(counter_id):
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = counter_pb2_grpc.CounterStub(channel)

        stub.Increment(
            counter_pb2.IncrementRequest(
                counter_id=counter_id,
                delta=1,
                idempotency_key=str(uuid.uuid4()),
                lamport_time=0
            ),
            timeout=5.0
        )


def quorum_increment(counter_id):
    manager = ReplicationManager(PORTS)

    result = manager.replicate(
        counter_id,
        1,
        str(uuid.uuid4())
    )

    if not result["success"]:
        raise RuntimeError("Quorum failed")


def benchmark(name, function, clients):
    print(f"Running: {name}")

    counter_id = "benchmark_" + str(uuid.uuid4())
    latencies = []

    def task(_):
        start = time.perf_counter()
        function(counter_id)
        end = time.perf_counter()

        return (end - start) * 1000

    with ThreadPoolExecutor(max_workers=clients) as executor:
        latencies = list(
            executor.map(task, range(REQUESTS))
        )

    latencies.sort()

    median = statistics.median(latencies)
    p95_index = math.ceil(0.95 * len(latencies)) - 1
    p95 = latencies[p95_index]

    print(f"Median: {median:.3f} ms")
    print(f"P95: {p95:.3f} ms")
    print(f"Requests: {len(latencies)}")
    print()

    return median, p95


def main():
    print("PERFORMANCE BENCHMARK")
    print("=====================")

    benchmark(
        "Single replica, 1 client",
        single_increment,
        1
    )

    benchmark(
        "Single replica, 16 clients",
        single_increment,
        16
    )

    benchmark(
        "Quorum (3 replicas), 1 client",
        quorum_increment,
        1
    )

    benchmark(
        "Quorum (3 replicas), 16 clients",
        quorum_increment,
        16
    )

    print("BENCHMARK COMPLETED")


if __name__ == "__main__":
    main()
