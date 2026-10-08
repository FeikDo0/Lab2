
import grpc

import counter_pb2
import counter_pb2_grpc

from recovery import ReplicaRecovery


ports = [50051, 50052, 50053]
counter_id = "replicated_counter"

# Создаём значение на первой реплике
with grpc.insecure_channel("localhost:50051") as channel:
    stub = counter_pb2_grpc.CounterStub(channel)

    response = stub.Increment(
        counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=30,
            idempotency_key="recovery-sync-demo-001",
            lamport_time=0
        ),
        timeout=2.0
    )

    print("Source counter:", response.new_value)

# Восстанавливаем остальные реплики
recovery = ReplicaRecovery(ports)

success = recovery.restore_replicas(
    counter_id,
    source_port=50051
)

assert success, "Recovery failed"

# Проверяем все реплики
values = []

for port in ports:
    with grpc.insecure_channel(
        f"localhost:{port}"
    ) as channel:

        stub = counter_pb2_grpc.CounterStub(channel)

        response = stub.Get(
            counter_pb2.GetRequest(
                counter_id=counter_id
            ),
            timeout=2.0
        )

        assert response.found
        values.append(response.value)

        print(f"Replica {port}: {response.value}")

assert len(set(values)) == 1

print("ALL REPLICAS CONSISTENT")
print("RECOVERY TEST PASSED")
