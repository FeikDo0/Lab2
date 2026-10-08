
import grpc
import counter_pb2
import counter_pb2_grpc


ports = [50051, 50052, 50053]
counter_id = "replicated_counter"

values = {}

for port in ports:
    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = counter_pb2_grpc.CounterStub(channel)

            response = stub.Get(
                counter_pb2.GetRequest(counter_id=counter_id),
                timeout=2.0
            )

            if response.found:
                values[port] = response.value
                print(f"Replica {port}: value={response.value}")
            else:
                print(f"Replica {port}: counter not found")

    except grpc.RpcError:
        print(f"Replica {port}: unavailable")

if len(values) == len(ports) and len(set(values.values())) == 1:
    print("ALL REPLICAS CONSISTENT")
else:
    print("REPLICAS NOT CONSISTENT")
